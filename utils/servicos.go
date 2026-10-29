package utils

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// formata a data para leitura (ex: "25/12/2026")
func (d Data) String() string {
	return fmt.Sprintf("%02d/%02d/%04d", d.Dia, d.Mes, d.Ano)
}

// é uma função utilitária para criar o tipo a partir de inteiros
func NovaData(dia, mes, ano int) Data {
	return Data{Dia: dia, Mes: mes, Ano: ano}
}

// representa um trecho de carona disponível na rede
type ArestaTrecho struct {
	CaronaID       string    `json:"carona_id"`
	MotoristaID    string    `json:"motorista_id"`
	Origem         string    `json:"origem"`
	Destino        string    `json:"destino"`
	HorarioPartida time.Time `json:"horario_partida"`
	HorarioChegada time.Time `json:"horario_chegada"`
	Preco          float64   `json:"preco"`
	AssentosLivre  int       `json:"assentos_livres"`
}

// Representa um itinerário completo sugerido ao passageiro (uma lista sequencial de trechos)
type Itinerario struct {
	Trechos    []ArestaTrecho `json:"trechos"`
	PrecoTotal float64        `json:"preco_total"`
}

// Grafo de Cidades
type GrafoCidades map[string][]ArestaTrecho

// constrói o grafo a partir das caronas cadastradas no JSON
func MontarGrafo(caronas []Carona, dataDesejada Data) GrafoCidades {
	grafo := make(GrafoCidades)

	for _, c := range caronas {
		if !c.Ativa {
			continue
		}
		for _, t := range c.Trechos {
			// Filtra pela data e assentos disponíveis
			if t.DataPartida == dataDesejada && t.AssentosLivre > 0 {
				aresta := ArestaTrecho{
					CaronaID:       c.ID,
					MotoristaID:    c.MotoristaID,
					Origem:         t.Origem,
					Destino:        t.Destino,
					HorarioPartida: t.HorarioPartida,
					HorarioChegada: t.HorarioChegada,
					Preco:          t.Preco,
					AssentosLivre:  t.AssentosLivre,
				}
				grafo[t.Origem] = append(grafo[t.Origem], aresta)
			}
		}
	}
	return grafo
}

// remove espaços nas extremidades e converte para caixa baixa
func NormalizarTexto(texto string) string {
	return strings.ToLower(strings.TrimSpace(texto))
}

// realiza a busca em largura comparando strings normalizados
func BuscarItinerarios(caronas []Carona, origem, destino string, data Data) []Itinerario {
	// Normaliza as entradas do passageiro
	origemNorm := NormalizarTexto(origem)
	destinoNorm := NormalizarTexto(destino)

	grafo := MontarGrafo(caronas, data)
	var resultados []Itinerario

	fila := [][]ArestaTrecho{}

	// Procura no grafo pelas chaves normalizadas
	for chaveOrigem, arestas := range grafo {
		if NormalizarTexto(chaveOrigem) == origemNorm {
			for _, aresta := range arestas {
				fila = append(fila, []ArestaTrecho{aresta})
			}
		}
	}

	for len(fila) > 0 {
		caminhoAtual := fila[0]
		fila = fila[1:]

		ultimoTrecho := caminhoAtual[len(caminhoAtual)-1]

		// Compara o destino final
		if NormalizarTexto(ultimoTrecho.Destino) == destinoNorm {
			var precoTotal float64
			for _, t := range caminhoAtual {
				precoTotal += t.Preco
			}

			caminhoConsolidado := ConsolidarTrechos(caminhoAtual)

			resultados = append(resultados, Itinerario{
				Trechos:    caminhoConsolidado,
				PrecoTotal: precoTotal,
			})
			continue
		}

		// Procura pelas conexões normalizando os destinos intermediários
		for chaveOrigem, proximasArestas := range grafo {
			if NormalizarTexto(chaveOrigem) == NormalizarTexto(ultimoTrecho.Destino) {
				for _, proxima := range proximasArestas {
					mesmaCarona := proxima.CaronaID == ultimoTrecho.CaronaID
					horarioValido := proxima.HorarioPartida.After(ultimoTrecho.HorarioChegada) || proxima.HorarioPartida.Equal(ultimoTrecho.HorarioChegada)

					if mesmaCarona || horarioValido {
						novoCaminho := append([]ArestaTrecho{}, caminhoAtual...)
						novoCaminho = append(novoCaminho, proxima)
						fila = append(fila, novoCaminho)
					}
				}
			}
		}
	}

	return resultados
}

// funde segmentos contínuos da mesma carona em um único trecho, pois estava printando mesmo motorista como vários trechos separados
func ConsolidarTrechos(trechos []ArestaTrecho) []ArestaTrecho {
	if len(trechos) <= 1 {
		return trechos
	}

	var consolidados []ArestaTrecho
	atual := trechos[0]

	for i := 1; i < len(trechos); i++ {
		proximo := trechos[i]

		// Se for a mesma carona e houver continuidade direta de rota
		if atual.CaronaID == proximo.CaronaID && atual.Destino == proximo.Origem {
			atual.Destino = proximo.Destino               // Estende o destino para o extremo final
			atual.HorarioChegada = proximo.HorarioChegada // Ajusta a chegada para o horário do último trecho
			atual.Preco += proximo.Preco                  // Soma o valor total da viagem contínua

			// Mantém o menor número de assentos disponíveis no percurso acumulado
			if proximo.AssentosLivre < atual.AssentosLivre {
				atual.AssentosLivre = proximo.AssentosLivre
			}
		} else {
			// Troca de carona/motorista: salva o trecho acumulado e inicia um novo
			consolidados = append(consolidados, atual)
			atual = proximo
		}
	}

	// Adiciona o último segmento processado
	consolidados = append(consolidados, atual)
	return consolidados
}

// persistencia de caronas
var (
	ArquivoCaronas  = "caronas.json"
	ArquivoReservas = "reservas.json"
	mu              sync.Mutex // Evita conflito de concorrência entre goroutines do servidor
	ArquivoUsuarios = "usuarios.json"
)

// lê todas as caronas salvas no arquivo JSON
func CarregarCaronas() ([]Carona, error) {
	// Se o arquivo não existir, retorna um slice vazio em vez de dar erro
	if _, err := os.Stat(ArquivoCaronas); os.IsNotExist(err) {
		return []Carona{}, nil
	}

	dados, err := os.ReadFile(ArquivoCaronas)
	if err != nil {
		return nil, err
	}

	if len(dados) == 0 {
		return []Carona{}, nil
	}

	var caronas []Carona
	err = json.Unmarshal(dados, &caronas)
	if err != nil {
		return nil, err
	}

	return caronas, nil
}

// adiciona uma nova carona ao arquivo JSON existente
func SalvarCarona(novaCarona Carona) error {
	mu.Lock()
	defer mu.Unlock() // Libera a trava do arquivo assim que a função terminar

	// Carrega as caronas já existentes
	caronas, err := CarregarCaronas()
	if err != nil {
		return err
	}

	//Adiciona a nova carona à lista
	caronas = append(caronas, novaCarona)

	//Converte a lista completa de volta para JSON
	dadosFormatados, err := json.MarshalIndent(caronas, "", "  ")
	if err != nil {
		return err
	}

	// salvaa
	//  os dados no arquivo
	return os.WriteFile(ArquivoCaronas, dadosFormatados, 0644)
}

// lê todas as reservas salvas
func CarregarReservas() ([]Reserva, error) {
	if _, err := os.Stat(ArquivoReservas); os.IsNotExist(err) {
		return []Reserva{}, nil
	}
	dados, err := os.ReadFile(ArquivoReservas)
	if err != nil || len(dados) == 0 {
		return []Reserva{}, nil
	}
	var reservas []Reserva
	err = json.Unmarshal(dados, &reservas)
	return reservas, err
}

// localiza, dentro dos trechos ORIGINAIS de uma carona, a sequência contínua de índices
// que corresponde a um trecho (possivelmente consolidado) de "origem" até "destino".
// Necessário porque ConsolidarTrechos funde vários trechos da mesma carona (ex: ssa->x->recife)
// em um único trecho exibido ao passageiro (ssa->recife), mas o arquivo caronas.json continua
// guardando os trechos separados. Sem isso, a validação/desconto de assentos nunca encontra
// o trecho consolidado e a reserva falha mesmo havendo assentos livres.
func localizarIndicesTrecho(carona Carona, origem, destino string) []int {
	for i, t := range carona.Trechos {
		if t.Origem != origem {
			continue
		}

		if t.Destino == destino {
			return []int{i}
		}

		// tenta estender a cadeia a partir daqui até bater exatamente com o destino desejado
		indices := []int{i}
		destinoAtual := t.Destino
		for j := i + 1; j < len(carona.Trechos); j++ {
			prox := carona.Trechos[j]
			if prox.Origem != destinoAtual {
				break
			}
			indices = append(indices, j)
			destinoAtual = prox.Destino
			if destinoAtual == destino {
				return indices
			}
		}
	}
	return nil
}

// executa a validação e reserva atômica de todos os trechos do itinerário
func ReservarItinerario(passageiroID string, itinerario Itinerario) (*Reserva, error) {
	mu.Lock()         // TRAVA EXCLUSIVA: Nenhuma outra goroutine lê ou altera assentos enquanto essa roda
	defer mu.Unlock() //Libera o arquivo apenas no final da operação

	// carrega o estado mais recente do arquivo de caronas
	caronas, err := CarregarCaronas()
	if err != nil {
		return nil, fmt.Errorf("erro ao carregar caronas: %v", err)
	}

	// Verifica se TODOS os trechos do itinerário ainda têm assento livre
	for _, trechoDesejado := range itinerario.Trechos {
		assentoDisponivel := false

		for _, c := range caronas {
			if c.ID == trechoDesejado.CaronaID && c.Ativa {
				indices := localizarIndicesTrecho(c, trechoDesejado.Origem, trechoDesejado.Destino)
				if indices != nil {
					todosDisponiveis := true
					for _, idx := range indices {
						if c.Trechos[idx].AssentosLivre <= 0 {
							todosDisponiveis = false
							break
						}
					}
					assentoDisponivel = todosDisponiveis
				}
			}
		}

		// Se QUALQUER um dos trechos do itinerário esgotou, a reserva falha por completo (Tudo ou Nada)
		if !assentoDisponivel {
			return nil, fmt.Errorf("reserva cancelada: o trecho de %s para %s não possui mais assentos disponíveis", trechoDesejado.Origem, trechoDesejado.Destino)
		}
	}

	//aplica a reserva atualizando a lista em memória
	for _, trechoDesejado := range itinerario.Trechos {
		for idxC, c := range caronas {
			if c.ID == trechoDesejado.CaronaID {
				indices := localizarIndicesTrecho(c, trechoDesejado.Origem, trechoDesejado.Destino)
				for _, idx := range indices {
					// Desconta o assento de cada perna original que compõe o trecho consolidado
					caronas[idxC].Trechos[idx].AssentosLivre--
				}
			}
		}
	}

	dadosCaronas, err := json.MarshalIndent(caronas, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("erro ao serializar caronas: %v", err)
	}
	if err := os.WriteFile(ArquivoCaronas, dadosCaronas, 0644); err != nil {
		return nil, fmt.Errorf("erro ao salvar caronas atualizadas: %v", err)
	}

	// Criar o registro da Reserva
	novaReserva := Reserva{
		ID:           fmt.Sprintf("RES-%d", time.Now().UnixNano()),
		PassageiroID: passageiroID,
		Itinerario:   itinerario.Trechos,
		ValorTotal:   itinerario.PrecoTotal,
		DataCriacao:  time.Now(),
	}
	// Salvar no reservas.json
	reservas, _ := CarregarReservas()
	reservas = append(reservas, novaReserva)
	dadosReservas, _ := json.MarshalIndent(reservas, "", "  ")
	if err := os.WriteFile(ArquivoReservas, dadosReservas, 0644); err != nil {
		return nil, fmt.Errorf("erro ao gravar reservas.json: %v", err)
	}

	return &novaReserva, nil
}

// retorna todas as reservas ativas de um determinado passageiro
func ListarReservasPassageiro(passageiroID string) ([]Reserva, error) {
	mu.Lock()
	defer mu.Unlock()

	reservas, err := CarregarReservas()
	if err != nil {
		return nil, err
	}

	var minhasReservas []Reserva
	for _, r := range reservas {
		if r.PassageiroID == passageiroID {
			minhasReservas = append(minhasReservas, r)
		}
	}
	return minhasReservas, nil
}

// remove a reserva e devolve os assentos para as caronas correspondentes
func CancelarReserva(reservaID, passageiroID string) error {
	mu.Lock()
	defer mu.Unlock()

	reservas, err := CarregarReservas()
	if err != nil {
		return err
	}

	reservaIDClean := strings.TrimSpace(reservaID)
	passageiroIDClean := strings.TrimSpace(passageiroID)

	var reservaEncontrada *Reserva
	novaListaReservas := make([]Reserva, 0)

	//Procura a reserva e valida a posse
	for _, r := range reservas {
		if strings.TrimSpace(r.ID) == reservaIDClean {
			if strings.TrimSpace(r.PassageiroID) != passageiroIDClean {
				return fmt.Errorf("você não tem permissão para cancelar esta reserva")
			}
			reservaEncontrada = &r
		} else {
			novaListaReservas = append(novaListaReservas, r)
		}
	}

	if reservaEncontrada == nil {
		return fmt.Errorf("reserva %s não encontrada", reservaIDClean)
	}

	//Devolve os assentos ocupados no caronas.json
	caronas, err := CarregarCaronas()
	if err == nil {
		for _, trechoReservado := range reservaEncontrada.Itinerario {
			caronaIDClean := strings.TrimSpace(trechoReservado.CaronaID)

			for idxC, c := range caronas {
				if strings.TrimSpace(c.ID) == caronaIDClean {
					indices := localizarIndicesTrecho(c, trechoReservado.Origem, trechoReservado.Destino)
					for _, idx := range indices {
						// Devolve o assento de cada perna original que compõe o trecho consolidado
						caronas[idxC].Trechos[idx].AssentosLivre++
					}
				}
			}
		}

		dadosCaronas, _ := json.MarshalIndent(caronas, "", "  ")
		_ = os.WriteFile(ArquivoCaronas, dadosCaronas, 0644)
	}

	// Atualiza o arquivo de reservas na raiz
	dadosReservas, err := json.MarshalIndent(novaListaReservas, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(ArquivoReservas, dadosReservas, 0644)
}

// traz todas as caronas cadastradas pelo motorista logado
func ConsultarCaronasMotorista(motoristaID string) ([]Carona, error) {
	mu.Lock()
	defer mu.Unlock()

	caronas, err := CarregarCaronas()
	if err != nil {
		return nil, err
	}

	var minhasCaronas []Carona
	for _, c := range caronas {
		if c.MotoristaID == motoristaID {
			minhasCaronas = append(minhasCaronas, c)
		}
	}
	return minhasCaronas, nil
}

// cancela a carona e verifica se há passageiros afetados
func CancelarCaronaMotorista(caronaID, motoristaID string, confirmouComPassageiros bool) (bool, string, error) {
	mu.Lock()
	defer mu.Unlock()

	caronas, err := CarregarCaronas()
	if err != nil {
		return false, "", err
	}

	// Localiza a carona e valida a posse do motorista
	var caronaIndex = -1
	for i, c := range caronas {
		if c.ID == caronaID {
			if c.MotoristaID != motoristaID {
				return false, "", fmt.Errorf("você não tem permissão para cancelar esta carona")
			}
			caronaIndex = i
			break
		}
	}

	if caronaIndex == -1 {
		return false, "", fmt.Errorf("carona %s não encontrada", caronaID)
	}

	// Checa se existem reservas associadas a esta carona
	reservas, _ := CarregarReservas()
	passageirosAfetados := make(map[string]bool)

	for _, r := range reservas {
		for _, trecho := range r.Itinerario {
			if trecho.CaronaID == caronaID {
				passageirosAfetados[r.PassageiroID] = true
			}
		}
	}

	qtdPassageiros := len(passageirosAfetados)

	// Se houver passageiros e o motorista ainda não confirmou expressamente o cancelamento
	if qtdPassageiros > 0 && !confirmouComPassageiros {
		msg := fmt.Sprintf("ATENÇÃO: Esta carona possui %d passageiro(s) com reserva confirmada! Deseja realmente cancelar?", qtdPassageiros)
		return true, msg, nil
	}

	caronas[caronaIndex].Ativa = false

	// Remove as reservas ativas dessa carona cancelada do reservas.json e devolve os assentos
	if qtdPassageiros > 0 {
		novasReservas := make([]Reserva, 0)
		for _, r := range reservas {
			temCaronaCancelada := false
			for _, trecho := range r.Itinerario {
				if trecho.CaronaID == caronaID {
					temCaronaCancelada = true
					break
				}
			}
			if temCaronaCancelada {
				for _, trechoReservado := range r.Itinerario {
					for idxC, c := range caronas {
						if strings.TrimSpace(c.ID) == strings.TrimSpace(trechoReservado.CaronaID) {
							indices := localizarIndicesTrecho(c, trechoReservado.Origem, trechoReservado.Destino)
							for _, idx := range indices {
								caronas[idxC].Trechos[idx].AssentosLivre++
							}
						}
					}
				}
			} else {
				novasReservas = append(novasReservas, r)
			}
		}
		dadosReservas, _ := json.MarshalIndent(novasReservas, "", "  ")
		_ = os.WriteFile(ArquivoReservas, dadosReservas, 0644)
	}

	dadosCaronas, _ := json.MarshalIndent(caronas, "", "  ")
	if err := os.WriteFile(ArquivoCaronas, dadosCaronas, 0644); err != nil {
		return false, "", err
	}

	return false, "Carona cancelada com sucesso!", nil
}

// retorna quais passageiros compraram assentos na carona
func ConsultarPassageirosCarona(caronaID, motoristaID string) ([]string, error) {
	mu.Lock()
	defer mu.Unlock()

	reservas, err := CarregarReservas()
	if err != nil {
		return nil, err
	}

	passageirosMap := make(map[string]bool)
	for _, r := range reservas {
		for _, trecho := range r.Itinerario {
			if trecho.CaronaID == caronaID {
				passageirosMap[r.PassageiroID] = true
			}
		}
	}

	var lista []string
	for p := range passageirosMap {
		lista = append(lista, p)
	}
	return lista, nil
}

// valida a senha do usuário ou cria um novo registro se não existir
func AutenticarOuCadastrarUsuario(email, senha string, tipo TipoUsuario) (bool, string, error) {
	mu.Lock()
	defer mu.Unlock()

	var usuarios []Usuario

	// Carrega os usuários existentes
	if _, err := os.Stat(ArquivoUsuarios); !os.IsNotExist(err) {
		dados, err := os.ReadFile(ArquivoUsuarios)
		if err == nil && len(dados) > 0 {
			_ = json.Unmarshal(dados, &usuarios)
		}
	}

	// Procura o usuário cadastrado
	for _, u := range usuarios {
		if u.Email == email {
			if u.Senha == senha {
				return true, "Autenticação realizada com sucesso!", nil
			}
			return false, "Senha incorreta.", nil
		}
	}

	//e não encontrou, realiza o cadastro automático no primeiro acesso
	novoUsuario := Usuario{
		Email: email,
		Senha: senha,
		Tipo:  tipo,
	}

	usuarios = append(usuarios, novoUsuario)
	dadosUsuarios, err := json.MarshalIndent(usuarios, "", "  ")
	if err != nil {
		return false, "Erro ao processar dados do usuário.", err
	}

	if err := os.WriteFile(ArquivoUsuarios, dadosUsuarios, 0644); err != nil {
		return false, "Erro ao salvar arquivo de usuários.", err
	}

	return true, "Usuário cadastrado e autenticado com sucesso!", nil
}

var (
	ArquivoLog = "servidor.log"
	muLog      sync.Mutex
)

// grava eventos, requisições e respostas com timestamp no arquivo de log
func RegistrarLog(formato string, v ...interface{}) {
	muLog.Lock()
	defer muLog.Unlock()

	timestamp := time.Now().Format("2006-01-02 15:04:05")
	mensagem := fmt.Sprintf(formato, v...)
	linhaLog := fmt.Sprintf("[%s] %s\n", timestamp, mensagem)

	// Abre o arquivo em modo append (ou cria se não existir)
	f, err := os.OpenFile(ArquivoLog, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("Erro ao gravar log: %v\n", err)
		return
	}
	defer f.Close()

	_, _ = f.WriteString(linhaLog)
}

var (
	usuariosLogados = make(map[string]bool)
	muSessoes       sync.Mutex
)

// tenta marcar o usuário como logado. Retorna falso se ele já estiver ativo.
func RegistrarLogin(email string) bool {
	muSessoes.Lock()
	defer muSessoes.Unlock()

	emailClean := strings.TrimSpace(strings.ToLower(email))
	if usuariosLogados[emailClean] {
		return false
	}
	usuariosLogados[emailClean] = true
	return true
}

// libera o usuário do mapa de sessões ativas ao desconectar
func RemoverLogin(email string) {
	muSessoes.Lock()
	defer muSessoes.Unlock()

	emailClean := strings.TrimSpace(strings.ToLower(email))
	delete(usuariosLogados, emailClean)
}
