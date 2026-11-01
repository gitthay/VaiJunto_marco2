package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"vaiJunto/utils"
)

var leitor = bufio.NewReader(os.Stdin)

func lerTexto(rotulo string) string {
	fmt.Print(rotulo)
	texto, _ := leitor.ReadString('\n')
	// Remove a quebra de linha (\n ou \r\n) e espaços extras antes e depois
	return strings.TrimSpace(texto)
}

// enviarRequisicaoTCP envia uma requisição ao servidor e detecta quedas de conexão (EOF / reset).
// Se o servidor tiver caído, avisa claramente ao usuário e tenta reconectar automaticamente
// caso o servidor já tenha voltado ao ar.
func enviarRequisicaoTCP(conexao *net.Conn, serverAddr, email, senha string, req utils.MensagemRequisicao, buf []byte) (utils.MensagemResposta, bool) {
	reqBytes, _ := json.Marshal(req)

	if *conexao != nil {
		_, errW := (*conexao).Write(reqBytes)
		if errW == nil {
			n, errR := (*conexao).Read(buf)
			if errR == nil && n > 0 {
				var resp utils.MensagemResposta
				_ = json.Unmarshal(buf[:n], &resp)
				return resp, true
			}
		}
		_ = (*conexao).Close()
		*conexao = nil
	}

	fmt.Printf("\n[ALERTA DE REDE]: A conexão com o servidor (%s) foi perdida! Tentando reconectar...\n", serverAddr)

	novaConn, errDial := net.DialTimeout("tcp", serverAddr, 2*time.Second)
	if errDial != nil {
		fmt.Printf("[SERVIDOR OFFLINE]: O servidor (%s) caiu e continua fora do ar no momento. Aguarde ele voltar e tente novamente.\n", serverAddr)
		return utils.MensagemResposta{}, false
	}

	// Servidor voltou: reautentica automaticamente a sessão do passageiro
	payloadAuth, _ := json.Marshal(map[string]interface{}{
		"email": email,
		"senha": senha,
		"tipo":  utils.TipoPassageiro,
	})
	reqAuth := utils.MensagemRequisicao{
		Acao:    utils.AcaoAutenticar,
		Usuario: email,
		Payload: payloadAuth,
	}
	reqAuthBytes, _ := json.Marshal(reqAuth)
	if _, err := novaConn.Write(reqAuthBytes); err != nil {
		_ = novaConn.Close()
		fmt.Printf("[SERVIDOR OFFLINE]: Falha ao restabelecer sessão com o servidor (%s).\n", serverAddr)
		return utils.MensagemResposta{}, false
	}
	nAuth, errAuth := novaConn.Read(buf)
	if errAuth != nil || nAuth == 0 {
		_ = novaConn.Close()
		fmt.Printf("[SERVIDOR OFFLINE]: O servidor (%s) não respondeu à reconexão.\n", serverAddr)
		return utils.MensagemResposta{}, false
	}

	// Reenvia a requisição original após reconectar com sucesso
	if _, err := novaConn.Write(reqBytes); err != nil {
		_ = novaConn.Close()
		fmt.Printf("[SERVIDOR OFFLINE]: Perda de conexão ao reenviar requisição para (%s).\n", serverAddr)
		return utils.MensagemResposta{}, false
	}
	n, err := novaConn.Read(buf)
	if err != nil || n == 0 {
		_ = novaConn.Close()
		fmt.Printf("[SERVIDOR OFFLINE]: Sem resposta do servidor (%s).\n", serverAddr)
		return utils.MensagemResposta{}, false
	}

	*conexao = novaConn
	fmt.Printf("[RECONECTADO]: Conexão restabelecida automaticamente com o servidor (%s)!\n", serverAddr)

	var resp utils.MensagemResposta
	_ = json.Unmarshal(buf[:n], &resp)
	return resp, true
}

// função auxiliar para consultar as reservas do cliente
func buscarReservas(conexao *net.Conn, serverAddr, email, senha string, buf []byte) ([]utils.Reserva, bool) {
	req := utils.MensagemRequisicao{Acao: utils.AcaoListarReservas, Usuario: email}
	resp, ok := enviarRequisicaoTCP(conexao, serverAddr, email, senha, req, buf)
	if !ok {
		return nil, false
	}

	var reservas []utils.Reserva
	_ = json.Unmarshal(resp.Payload, &reservas)
	return reservas, true
}

// exibirDetalhesReserva imprime a reserva e cada um de seus trechos, indicando se algum servidor participante está offline
func exibirDetalhesReserva(indice int, r utils.Reserva) {
	origemGeral := "[Servidor Offline]"
	destinoGeral := "[Servidor Offline]"
	dataHoraStr := "Consultando servidor..."

	if len(r.Itinerario) > 0 {
		if r.Itinerario[0].Origem != "" {
			origemGeral = r.Itinerario[0].Origem
		}
		if r.Itinerario[len(r.Itinerario)-1].Destino != "" {
			destinoGeral = r.Itinerario[len(r.Itinerario)-1].Destino
		}
		if !r.Itinerario[0].HorarioPartida.IsZero() && !r.Itinerario[0].ServidorOffline {
			dataHoraStr = fmt.Sprintf("%s às %s",
				r.Itinerario[0].HorarioPartida.Format("02/01/2006"),
				r.Itinerario[0].HorarioPartida.Format("15:04"))
		} else if r.Itinerario[0].ServidorOffline {
			dataHoraStr = "[Servidor do 1º trecho OFFLINE]"
		}
	}

	fmt.Printf("\n[%d] ID Reserva: %s | Rota: %s -> %s\n", indice, r.ID, origemGeral, destinoGeral)
	fmt.Printf("    Partida: %s | Valor Total: R$ %.2f | Trechos: %d\n", dataHoraStr, r.ValorTotal, len(r.Itinerario))

	for j, t := range r.Itinerario {
		emp := t.EmpresaID
		if emp == "" {
			emp = "local"
		}
		if t.ServidorOffline {
			fmt.Printf("      Trecho %d [%s]: (Carona ID: %s | Servidor: %s) -> [SERVIDOR OFFLINE - Detalhes indisponíveis no momento]\n",
				j+1, emp, t.CaronaID, t.ServidorURL)
		} else {
			fmt.Printf("      Trecho %d [%s]: %s -> %s (Carona ID: %s | Motorista: %s | Partida: %s | Chegada: %s | R$ %.2f)\n",
				j+1, emp, t.Origem, t.Destino, t.CaronaID, t.MotoristaID,
				t.HorarioPartida.Format("15:04"), t.HorarioChegada.Format("15:04"), t.Preco)
		}
	}
}

func main() {
	// Flag para aceitar IP dinâmico na execução do terminal
	serverAddr := flag.String("server", "localhost:8080", "Endereço do servidor TCP (ex: 192.168.1.15:8080)")
	flag.Parse()

	// Conecta ao endereço configurado
	conexao, err := net.Dial("tcp", *serverAddr)
	if err != nil {
		log.Fatalln("Erro ao conectar ao servidor:", err)
	}
	defer func() {
		if conexao != nil {
			_ = conexao.Close()
		}
	}()

	buf := make([]byte, 16384)

	fmt.Println("==========================================")
	fmt.Println("        VAIJUNTO - PASSAGEIRO             ")
	fmt.Println("==========================================")

	//AUTENTICAÇÃO
	var email, senha string
	for {
		email = lerTexto("Digite seu Email: ")
		senha = lerTexto("Digite sua Senha: ")

		payloadAuth, _ := json.Marshal(map[string]interface{}{
			"email": email,
			"senha": senha,
			"tipo":  utils.TipoPassageiro,
		})

		reqAuth := utils.MensagemRequisicao{
			Acao:    utils.AcaoAutenticar,
			Usuario: email,
			Payload: payloadAuth,
		}
		reqAuthBytes, _ := json.Marshal(reqAuth)
		_, _ = conexao.Write(reqAuthBytes)

		n, err := conexao.Read(buf)
		if err != nil {
			log.Fatalln("Erro de comunicação com o servidor (servidor caiu):", err)
		}

		var respAuth utils.MensagemResposta
		_ = json.Unmarshal(buf[:n], &respAuth)

		if !respAuth.Sucesso {
			fmt.Printf("\n[ERRO]: %s Tente novamente.\n\n", respAuth.Mensagem)
			continue
		}

		fmt.Println("\n[SUCESSO]:", respAuth.Mensagem)
		break
	}

	// MENU PRINCIPAL
	for {
		fmt.Println("\n------------------------------------------")
		fmt.Println("1. Buscar/Reservar Caronas")
		fmt.Println("2. Minhas Reservas Ativas")
		fmt.Println("3. Cancelar Reserva")
		fmt.Println("0. Sair")
		fmt.Println("------------------------------------------")

		opcao := lerTexto("Escolha uma opção: ")

		switch opcao {
		case "1":
			fmt.Println("\n--- BUSCAR ITINERÁRIOS (Digite 0 em qualquer campo para voltar) ---")
			origem := lerTexto("Origem: ")
			if origem == "0" {
				continue
			}

			destino := lerTexto("Destino: ")
			if destino == "0" {
				continue
			}

			// Leitura e validação da data
			var dataFiltro utils.Data
			cancelarBusca := false
			for {
				dataStr := lerTexto("Data da viagem (DD/MM/AAAA): ")
				if dataStr == "0" {
					cancelarBusca = true
					break
				}
				partes := strings.Split(dataStr, "/")
				if len(partes) != 3 {
					fmt.Println("[ERRO]: Formato inválido. Use DD/MM/AAAA. Tente novamente.")
					continue
				}
				d, errD := strconv.Atoi(partes[0])
				m, errM := strconv.Atoi(partes[1])
				a, errA := strconv.Atoi(partes[2])
				if errD != nil || errM != nil || errA != nil || d < 1 || d > 31 || m < 1 || m > 12 {
					fmt.Println("[ERRO]: Data inválida. Tente novamente.")
					continue
				}
				dataFiltro = utils.NovaData(d, m, a)
				break
			}
			if cancelarBusca {
				continue
			}

			filtro := struct {
				Origem  string     `json:"origem"`
				Destino string     `json:"destino"`
				Data    utils.Data `json:"data"`
			}{
				Origem:  origem,
				Destino: destino,
				Data:    dataFiltro,
			}

			payload, _ := json.Marshal(filtro)
			req := utils.MensagemRequisicao{Acao: utils.AcaoBuscarItinerario, Usuario: email, Payload: payload}
			resp, ok := enviarRequisicaoTCP(&conexao, *serverAddr, email, senha, req, buf)
			if !ok {
				continue
			}

			var itinerariosEncontrados []utils.Itinerario
			_ = json.Unmarshal(resp.Payload, &itinerariosEncontrados)

			fmt.Println("\n==========================================")
			fmt.Printf("   %s\n", resp.Mensagem)
			fmt.Println("==========================================")

			if len(itinerariosEncontrados) == 0 {
				fmt.Println("Nenhum itinerário encontrado para a rota e data especificadas.")
				continue
			}

			for i, itin := range itinerariosEncontrados {
				fmt.Printf("\nOPÇÃO [%d] - Preço Total: R$ %.2f\n", i+1, itin.PrecoTotal)
				for j, t := range itin.Trechos {
					emp := t.EmpresaID
					if emp == "" {
						emp = "local"
					}
					fmt.Printf("  Trecho %d [%s]: %s -> %s | Partida: %s | Chegada: %s (Carona ID: %s | Motorista: %s)\n",
						j+1, emp, t.Origem, t.Destino, t.HorarioPartida.Format("15:04"), t.HorarioChegada.Format("15:04"), t.CaronaID, t.MotoristaID)
				}
			}

			// Pergunta se deseja reservar ou voltar (repete se digitar algo inválido)
			fmt.Println("\n------------------------------------------")
			var desejaReservar string
			for {
				desejaReservar = strings.ToLower(lerTexto("Deseja reservar algum desses itinerários? (s/n): "))
				if desejaReservar == "s" || desejaReservar == "n" {
					break
				}
				fmt.Println("[ERRO]: Digite apenas 's' para sim ou 'n' para não.")
			}

			if desejaReservar == "n" {
				continue
			}

			input := lerTexto("Digite o número da OPÇÃO desejada (ou 0 para cancelar): ")
			num, err := strconv.Atoi(input)
			if err != nil || num == 0 || num > len(itinerariosEncontrados) {
				fmt.Println("Reserva não realizada.")
				continue
			}

			itinerarioEscolhido := itinerariosEncontrados[num-1]
			payloadReserva, _ := json.Marshal(itinerarioEscolhido)

			reqReserva := utils.MensagemRequisicao{Acao: utils.AcaoReservarTrecho, Usuario: email, Payload: payloadReserva}
			respReserva, ok := enviarRequisicaoTCP(&conexao, *serverAddr, email, senha, reqReserva, buf)
			if !ok {
				continue
			}

			fmt.Println("\n>", respReserva.Mensagem)

		case "2":
			reservas, ok := buscarReservas(&conexao, *serverAddr, email, senha, buf)
			if !ok {
				continue
			}
			fmt.Println("\n--- MINHAS RESERVAS (CONSULTA EM TEMPO REAL AO CONSÓRCIO) ---")
			if len(reservas) == 0 {
				fmt.Println("Você não possui reservas ativas.")
				continue
			}
			for i, r := range reservas {
				exibirDetalhesReserva(i+1, r)
			}

		case "3":
			reservas, ok := buscarReservas(&conexao, *serverAddr, email, senha, buf)
			if !ok {
				continue
			}
			fmt.Println("\n==========================================")
			fmt.Println("           CANCELAR RESERVA               ")
			fmt.Println("==========================================")

			if len(reservas) == 0 {
				fmt.Println("Você não possui reservas ativas para cancelar.")
				continue
			}

			// Lista todas as reservas com detalhes completos (ou status offline do servidor parceiro)
			for i, r := range reservas {
				exibirDetalhesReserva(i+1, r)
			}

			fmt.Println("\n[0] Voltar ao menu")

			input := lerTexto("Escolha o número da reserva para cancelar: ")
			num, err := strconv.Atoi(input)
			if err != nil || num == 0 || num > len(reservas) {
				continue
			}

			reservaEscolhida := reservas[num-1]

			// Confirmação antes de enviar o cancelamento
			confirmacao := lerTexto(fmt.Sprintf("Deseja realmente cancelar a reserva %s? (s/n): ", reservaEscolhida.ID))
			if strings.ToLower(confirmacao) != "s" {
				fmt.Println("Operação cancelada.")
				continue
			}

			payload, _ := json.Marshal(map[string]string{"reserva_id": reservaEscolhida.ID})

			req := utils.MensagemRequisicao{Acao: utils.AcaoCancelarReserva, Usuario: email, Payload: payload}
			resp, ok := enviarRequisicaoTCP(&conexao, *serverAddr, email, senha, req, buf)
			if !ok {
				continue
			}

			fmt.Println("\n>", resp.Mensagem)

		case "0":
			fmt.Println("Saindo...")
			return
		}
	}
}
