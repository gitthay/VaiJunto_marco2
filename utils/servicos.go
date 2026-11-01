package utils

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
	CaronaID        string    `json:"carona_id"`
	MotoristaID     string    `json:"motorista_id,omitempty"`
	EmpresaID       string    `json:"empresa_id,omitempty"`
	ServidorURL     string    `json:"servidor_url,omitempty"`
	Origem          string    `json:"origem,omitempty"`
	Destino         string    `json:"destino,omitempty"`
	HorarioPartida  time.Time `json:"horario_partida,omitempty"`
	HorarioChegada  time.Time `json:"horario_chegada,omitempty"`
	Preco           float64   `json:"preco,omitempty"`
	AssentosLivre   int       `json:"assentos_livres,omitempty"`
	ServidorOffline bool      `json:"servidor_offline,omitempty"`
}

// MarshalJSON garante que trechos de outros servidores salvos localmente no reservas.json
// contenham apenas as referências (carona_id, empresa_id, servidor_url),
// sem guardar origem, destino, motorista, preço ou horários de outro servidor.
func (a ArestaTrecho) MarshalJSON() ([]byte, error) {
	if a.MotoristaID == "" && a.HorarioPartida.IsZero() && a.HorarioChegada.IsZero() && a.Preco == 0 {
		return json.Marshal(struct {
			CaronaID        string `json:"carona_id"`
			EmpresaID       string `json:"empresa_id,omitempty"`
			ServidorURL     string `json:"servidor_url,omitempty"`
			Origem          string `json:"origem,omitempty"`
			Destino         string `json:"destino,omitempty"`
			ServidorOffline bool   `json:"servidor_offline,omitempty"`
		}{
			CaronaID:        a.CaronaID,
			EmpresaID:       a.EmpresaID,
			ServidorURL:     a.ServidorURL,
			Origem:          a.Origem,
			Destino:         a.Destino,
			ServidorOffline: a.ServidorOffline,
		})
	}
	type Alias ArestaTrecho
	return json.Marshal((Alias)(a))
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
					EmpresaID:      EmpresaAtualID,
					ServidorURL:    ServidorAtualURL,
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

// realiza a busca em largura comparando strings normalizados (local + consórcio quando houver parceiros)
func BuscarItinerarios(caronas []Carona, origem, destino string, data Data) []Itinerario {
	muPeers.RLock()
	temPeers := len(PeersConsorcio) > 0
	muPeers.RUnlock()

	if temPeers {
		return BuscarItinerariosConsorcio(caronas, origem, destino, data)
	}

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

		// Se for a mesma carona (na mesma empresa) e houver continuidade direta de rota
		if atual.CaronaID == proximo.CaronaID && atual.ServidorURL == proximo.ServidorURL && atual.Destino == proximo.Origem {
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

// persistencia de caronas e isolamento por instancia de empresa (apenas os 3 arquivos originais)
var (
	EmpresaAtualID   = "vaijunto"
	ServidorAtualURL = "http://localhost:9080"
	ArquivoCaronas   = "caronas.json"
	ArquivoReservas  = "reservas.json"
	mu               sync.Mutex // Evita conflito de concorrência entre goroutines do servidor
	ArquivoUsuarios  = "usuarios.json"
)

// ConfigurarInstancia define a identidade da empresa e cria APENAS a pasta e os arquivos JSON
// do servidor correspondente que está sendo iniciado nesta máquina.
func ConfigurarInstancia(empresaID, servidorURL string) error {
	if empresaID != "" {
		EmpresaAtualID = empresaID
	}
	if servidorURL != "" {
		ServidorAtualURL = strings.TrimRight(servidorURL, "/")
	}

	if empresaID != "" && empresaID != "default" {
		pasta := filepath.Join("dados", empresaID)
		if err := os.MkdirAll(pasta, 0755); err != nil {
			return err
		}
		ArquivoCaronas = filepath.Join(pasta, "caronas.json")
		ArquivoReservas = filepath.Join(pasta, "reservas.json")
		ArquivoUsuarios = filepath.Join(pasta, "usuarios.json")
		ArquivoLog = filepath.Join(pasta, "servidor.log")
	}

	// Garante que os 3 arquivos JSON desta instância existam imediatamente ao subir o servidor
	for _, arq := range []string{ArquivoCaronas, ArquivoReservas, ArquivoUsuarios} {
		if _, err := os.Stat(arq); os.IsNotExist(err) {
			if err := os.WriteFile(arq, []byte("[]"), 0644); err != nil {
				return err
			}
		}
	}
	return nil
}

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

	// salva os dados no arquivo
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

// FiltrarReservaParaArmazenamentoLocal descentraliza os dados da reserva:
// - Para trechos pertencentes a ESTE servidor (ServidorAtualURL), preenche/mantém os detalhes completos locais.
// - Para trechos pertencentes a OUTROS servidores participantes, armazena APENAS as informações de referência
//   (CaronaID, EmpresaID, ServidorURL, Origem, Destino), obrigando a consulta via rede quando detalhes forem solicitados.
func FiltrarReservaParaArmazenamentoLocal(r Reserva) Reserva {
	copia := r
	copia.Itinerario = make([]ArestaTrecho, len(r.Itinerario))
	caronasLocais, _ := CarregarCaronas()

	for i, tr := range r.Itinerario {
		urlSrv := strings.TrimRight(strings.TrimSpace(tr.ServidorURL), "/")
		if urlSrv == "" {
			urlSrv = ServidorAtualURL
		}

		if urlSrv == ServidorAtualURL {
			trechoLocal := tr
			trechoLocal.ServidorURL = ServidorAtualURL
			if trechoLocal.EmpresaID == "" {
				trechoLocal.EmpresaID = EmpresaAtualID
			}
			// Se veio apenas com referência, hidrata com os dados da carona local
			if trechoLocal.MotoristaID == "" || trechoLocal.HorarioPartida.IsZero() {
				if det, ok := obterDetalheTrechoEmListaCaronas(caronasLocais, trechoLocal.CaronaID, trechoLocal.Origem, trechoLocal.Destino); ok {
					trechoLocal.MotoristaID = det.MotoristaID
					trechoLocal.HorarioPartida = det.HorarioPartida
					trechoLocal.HorarioChegada = det.HorarioChegada
					trechoLocal.Preco = det.Preco
				}
			}
			trechoLocal.AssentosLivre = 0 // Não persiste snapshot estático de assentos livres na reserva
			trechoLocal.ServidorOffline = false
			copia.Itinerario[i] = trechoLocal
		} else {
			// Trecho de outro servidor: guarda somente quem é o servidor participante e o ID da corrida
			copia.Itinerario[i] = ArestaTrecho{
				CaronaID:    tr.CaronaID,
				EmpresaID:   tr.EmpresaID,
				ServidorURL: urlSrv,
			}
		}
	}
	return copia
}

// obterDetalheTrechoEmListaCaronas extrai os dados consolidados de um trecho (origem -> destino) de uma carona local
func obterDetalheTrechoEmListaCaronas(caronas []Carona, caronaID, origem, destino string) (ArestaTrecho, bool) {
	caronaIDClean := strings.TrimSpace(caronaID)
	for _, c := range caronas {
		if strings.TrimSpace(c.ID) == caronaIDClean && c.Ativa {
			indices := localizarIndicesTrecho(c, origem, destino)
			if len(indices) == 0 {
				continue
			}
			primeiro := c.Trechos[indices[0]]
			ultimo := c.Trechos[indices[len(indices)-1]]
			var precoTotal float64
			menorAssento := primeiro.AssentosLivre
			for _, idx := range indices {
				precoTotal += c.Trechos[idx].Preco
				if c.Trechos[idx].AssentosLivre < menorAssento {
					menorAssento = c.Trechos[idx].AssentosLivre
				}
			}
			return ArestaTrecho{
				CaronaID:       c.ID,
				MotoristaID:    c.MotoristaID,
				EmpresaID:      EmpresaAtualID,
				ServidorURL:    ServidorAtualURL,
				Origem:         origem,
				Destino:        destino,
				HorarioPartida: primeiro.HorarioPartida,
				HorarioChegada: ultimo.HorarioChegada,
				Preco:          precoTotal,
				AssentosLivre:  menorAssento,
			}, true
		}
	}
	return ArestaTrecho{}, false
}

// SalvarOuAtualizarReservaLocalSemLock grava ou atualiza a reserva descentralizada no reservas.json local
// (assume que mu.Lock() já está adquirido pelo chamador)
func SalvarOuAtualizarReservaLocalSemLock(r Reserva) error {
	reservaFiltrada := FiltrarReservaParaArmazenamentoLocal(r)
	reservas, _ := CarregarReservas()

	atualizou := false
	for i := range reservas {
		if strings.TrimSpace(reservas[i].ID) == strings.TrimSpace(reservaFiltrada.ID) {
			reservas[i] = reservaFiltrada
			atualizou = true
			break
		}
	}
	if !atualizou {
		reservas = append(reservas, reservaFiltrada)
	}

	dadosReservas, err := json.MarshalIndent(reservas, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(ArquivoReservas, dadosReservas, 0644)
}

// localiza, dentro dos trechos ORIGINAIS de uma carona, a sequência contínua de índices
// que corresponde a um trecho (possivelmente consolidado) de "origem" até "destino".
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

// executa a validação e reserva atômica de todos os trechos do itinerário (via Two-Phase Commit 2PC)
func ReservarItinerario(passageiroID string, itinerario Itinerario) (*Reserva, error) {
	return CoordenarReserva2PC(passageiroID, itinerario)
}

// retorna todas as reservas ativas de um determinado passageiro, buscando sob demanda via REST
// os detalhes dos trechos que pertencem a outros servidores participantes do consórcio
func ListarReservasPassageiro(passageiroID string) ([]Reserva, error) {
	// Antes de listar, reconcilia eventuais cancelamentos ocorridos enquanto servidores estavam offline
	sincronizarReservasComPeers()

	mu.Lock()
	reservas, err := CarregarReservas()
	mu.Unlock()
	if err != nil {
		return nil, err
	}

	var minhasReservas []Reserva
	for _, r := range reservas {
		if r.PassageiroID == passageiroID {
			copia := r
			PreencherDetalhesRemotosReserva(&copia)
			minhasReservas = append(minhasReservas, copia)
		}
	}
	return minhasReservas, nil
}

// remove a reserva e devolve os assentos para as caronas correspondentes (locais e remotas no consórcio via REST)
func CancelarReserva(reservaID, passageiroID string) error {
	mu.Lock()

	reservas, err := CarregarReservas()
	if err != nil {
		mu.Unlock()
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
				mu.Unlock()
				return fmt.Errorf("você não tem permissão para cancelar esta reserva")
			}
			copia := r
			reservaEncontrada = &copia
		} else {
			novaListaReservas = append(novaListaReservas, r)
		}
	}

	if reservaEncontrada == nil {
		mu.Unlock()
		return fmt.Errorf("reserva %s não encontrada", reservaIDClean)
	}

	trechosRemotosPorServidor := make(map[string][]ArestaTrecho)
	urlCoordenador := strings.TrimRight(strings.TrimSpace(reservaEncontrada.CoordenadorURL), "/")
	if urlCoordenador != "" && urlCoordenador != ServidorAtualURL {
		trechosRemotosPorServidor[urlCoordenador] = []ArestaTrecho{}
	}

	//Devolve os assentos ocupados no caronas.json local e separa trechos de outros servidores
	caronas, err := CarregarCaronas()
	if err == nil {
		for _, trechoReservado := range reservaEncontrada.Itinerario {
			urlSrv := strings.TrimRight(strings.TrimSpace(trechoReservado.ServidorURL), "/")
			if urlSrv != "" && urlSrv != ServidorAtualURL {
				trechosRemotosPorServidor[urlSrv] = append(trechosRemotosPorServidor[urlSrv], trechoReservado)
				continue
			}

			caronaIDClean := strings.TrimSpace(trechoReservado.CaronaID)
			for idxC, c := range caronas {
				if strings.TrimSpace(c.ID) == caronaIDClean {
					indices := localizarIndicesTrecho(c, trechoReservado.Origem, trechoReservado.Destino)
					for _, idx := range indices {
						// Devolve o assento de cada perna original que compõe o trecho consolidado
						if caronas[idxC].Trechos[idx].AssentosLivre < caronas[idxC].AssentosTot {
							caronas[idxC].Trechos[idx].AssentosLivre++
						}
					}
				}
			}
		}

		dadosCaronas, _ := json.MarshalIndent(caronas, "", "  ")
		_ = os.WriteFile(ArquivoCaronas, dadosCaronas, 0644)
	}

	// Marca em memória como abortada caso estivesse registrada na sessão
	marcarReservaAbortadaEmMemoria(reservaIDClean)

	// Atualiza o arquivo de reservas
	dadosReservas, err := json.MarshalIndent(novaListaReservas, "", "  ")
	if err != nil {
		mu.Unlock()
		return err
	}
	err = os.WriteFile(ArquivoReservas, dadosReservas, 0644)
	mu.Unlock()

	// Notifica via requisição REST os demais servidores do consórcio para liberarem seus assentos e removerem a reserva
	for urlSrv, trechos := range trechosRemotosPorServidor {
		EnviarLiberacaoRemota(urlSrv, reservaIDClean, trechos)
	}

	return err
}

// traz todas as caronas cadastradas pelo motorista logado
func ConsultarCaronasMotorista(motoristaID string) ([]Carona, error) {
	sincronizarReservasComPeers()

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

// cancela a carona e verifica se há passageiros afetados (locais e de outras empresas via requisição REST)
func CancelarCaronaMotorista(caronaID, motoristaID string, confirmouComPassageiros bool) (bool, string, error) {
	sincronizarReservasComPeers()

	// Primeiro consulta via REST quais passageiros de empresas parceiras possuem reserva nesta carona
	passageirosRemotos := ConsultarPassageirosRemotosDaCarona(caronaID)

	mu.Lock()

	caronas, err := CarregarCaronas()
	if err != nil {
		mu.Unlock()
		return false, "", err
	}

	// Localiza a carona e valida a posse do motorista
	var caronaIndex = -1
	for i, c := range caronas {
		if c.ID == caronaID {
			if c.MotoristaID != motoristaID {
				mu.Unlock()
				return false, "", fmt.Errorf("você não tem permissão para cancelar esta carona")
			}
			caronaIndex = i
			break
		}
	}

	if caronaIndex == -1 {
		mu.Unlock()
		return false, "", fmt.Errorf("carona %s não encontrada", caronaID)
	}

	// Checa reservas locais e soma com os passageiros remotos consultados via REST
	reservas, _ := CarregarReservas()
	passageirosAfetados := make(map[string]bool)

	for _, r := range reservas {
		for _, trecho := range r.Itinerario {
			if trecho.CaronaID == caronaID && (trecho.ServidorURL == "" || trecho.ServidorURL == ServidorAtualURL) {
				passageirosAfetados[r.PassageiroID] = true
			}
		}
	}
	for _, pRemoto := range passageirosRemotos {
		passageirosAfetados[pRemoto] = true
	}

	qtdPassageiros := len(passageirosAfetados)

	// Se houver passageiros e o motorista ainda não confirmou expressamente o cancelamento
	if qtdPassageiros > 0 && !confirmouComPassageiros {
		mu.Unlock()
		msg := fmt.Sprintf("ATENÇÃO: Esta carona possui %d passageiro(s) com reserva confirmada! Deseja realmente cancelar?", qtdPassageiros)
		return true, msg, nil
	}

	caronas[caronaIndex].Ativa = false

	type acaoRemota struct {
		url       string
		reservaID string
		trechos   []ArestaTrecho
	}
	var liberacoesRemotas []acaoRemota

	// Remove as reservas ativas locais dessa carona cancelada e devolve os assentos
	if len(reservas) > 0 {
		novasReservas := make([]Reserva, 0)
		for _, r := range reservas {
			temCaronaCancelada := false
			for _, trecho := range r.Itinerario {
				if trecho.CaronaID == caronaID && (trecho.ServidorURL == "" || trecho.ServidorURL == ServidorAtualURL) {
					temCaronaCancelada = true
					break
				}
			}
			if temCaronaCancelada {
				marcarReservaAbortadaEmMemoria(r.ID)
				for _, trechoReservado := range r.Itinerario {
					urlSrv := strings.TrimRight(strings.TrimSpace(trechoReservado.ServidorURL), "/")
					if urlSrv != "" && urlSrv != ServidorAtualURL {
						liberacoesRemotas = append(liberacoesRemotas, acaoRemota{
							url:       urlSrv,
							reservaID: r.ID,
							trechos:   []ArestaTrecho{trechoReservado},
						})
						continue
					}
					for idxC, c := range caronas {
						if strings.TrimSpace(c.ID) == strings.TrimSpace(trechoReservado.CaronaID) {
							indices := localizarIndicesTrecho(c, trechoReservado.Origem, trechoReservado.Destino)
							for _, idx := range indices {
								if caronas[idxC].Trechos[idx].AssentosLivre < caronas[idxC].AssentosTot {
									caronas[idxC].Trechos[idx].AssentosLivre++
								}
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
		mu.Unlock()
		return false, "", err
	}
	mu.Unlock()

	// Notifica via requisição REST todos os servidores parceiros do consórcio sobre o cancelamento desta carona
	NotificarCancelamentoCaronaAosPeers(caronaID)

	for _, lib := range liberacoesRemotas {
		EnviarLiberacaoRemota(lib.url, lib.reservaID, lib.trechos)
	}

	return false, "Carona cancelada com sucesso!", nil
}

// retorna quais passageiros compraram assentos na carona (locais + consulta REST às empresas parceiras)
func ConsultarPassageirosCarona(caronaID, motoristaID string) ([]string, error) {
	sincronizarReservasComPeers()
	passageirosRemotos := ConsultarPassageirosRemotosDaCarona(caronaID)

	mu.Lock()
	defer mu.Unlock()

	reservas, err := CarregarReservas()
	if err != nil {
		return nil, err
	}

	passageirosMap := make(map[string]bool)
	for _, r := range reservas {
		for _, trecho := range r.Itinerario {
			if trecho.CaronaID == caronaID && (trecho.ServidorURL == "" || trecho.ServidorURL == ServidorAtualURL) {
				passageirosMap[r.PassageiroID] = true
			}
		}
	}

	for _, p := range passageirosRemotos {
		passageirosMap[p] = true
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

	// se não encontrou, realiza o cadastro automático no primeiro acesso
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
