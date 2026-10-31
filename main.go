package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"

	"vaiJunto/utils"
)

func gerenciarConexao(conexao net.Conn) {
	defer conexao.Close()
	clienteAddr := conexao.RemoteAddr().String()
	utils.RegistrarLog("[CONEXAO] Novo cliente conectado de: %s", clienteAddr)

	var usuarioLogadoNestaConexao string

	// Garante que se o cliente cair/desconectar, o login é liberado 
	defer func() {
		if usuarioLogadoNestaConexao != "" {
			utils.RemoverLogin(usuarioLogadoNestaConexao)
			utils.RegistrarLog("[DESCONEXAO] Sessão ativa do usuário %s liberada.", usuarioLogadoNestaConexao)
		}
	}()

	buf := make([]byte, 2048)
	for {
		n, err := conexao.Read(buf)
		if err != nil {
			if err == io.EOF {
				fmt.Println("Cliente desconectou.")
				utils.RegistrarLog("[DESCONEXAO] Cliente %s desconectou normalmente.", clienteAddr)
				return
			}
			log.Println("Erro ao ler da conexão:", err)
			utils.RegistrarLog("[ERRO CONEXAO] Erro na leitura do cliente %s: %v", clienteAddr, err)
			return
		}

		var req utils.MensagemRequisicao
		if err := json.Unmarshal(buf[:n], &req); err != nil {
			log.Println("JSON inválido:", err)
			utils.RegistrarLog("[ERRO JSON] Cliente %s enviou JSON malformado: %s", clienteAddr, string(buf[:n]))
			return
		}

		// Log da Requisição
		utils.RegistrarLog("[REQUISICAO] De: %s | Usuario: %s | Acao: %s | Payload: %s",
			clienteAddr, req.Usuario, req.Acao, string(req.Payload))

		// Função auxiliar interna para responder e logar
		enviarResposta := func(resp utils.MensagemResposta) {
			respBytes, _ := json.Marshal(resp)
			conexao.Write(respBytes)
			utils.RegistrarLog("[RESPOSTA] Para: %s (Usuario: %s) | Acao: %s | Sucesso: %t | Msg: %s | Payload: %s",
				clienteAddr, req.Usuario, req.Acao, resp.Sucesso, resp.Mensagem, string(resp.Payload))
		}

		switch req.Acao {
		case utils.AcaoPublicarCarona:
			var carona utils.Carona
			if err := json.Unmarshal(req.Payload, &carona); err != nil {
				log.Println("Erro ao ler payload da carona:", err)
				return
			}

			if err := utils.SalvarCarona(carona); err != nil {
				log.Println("Erro ao salvar carona:", err)

				resp := utils.MensagemResposta{
					Sucesso:  false,
					Mensagem: "Erro ao salvar carona no servidor.",
				}
				respBytes, _ := json.Marshal(resp)
				conexao.Write(respBytes)
				return
			}

			fmt.Println("Carona cadastrada e gravada no arquivo caronas.json!")

			resp := utils.MensagemResposta{
				Sucesso:  true,
				Mensagem: "Carona cadastrada com sucesso!",
			}
			respBytes, _ := json.Marshal(resp)
			conexao.Write(respBytes)

		case utils.AcaoBuscarItinerario:
			// Decodifica os parâmetros de busca enviados pelo passageiro
			var filtro struct {
				Origem  string     `json:"origem"`
				Destino string     `json:"destino"`
				Data    utils.Data `json:"data"`
			}

			if err := json.Unmarshal(req.Payload, &filtro); err != nil {
				log.Println("Erro no payload da busca:", err)
				return
			}

			//Carrega as caronas do JSON
			caronas, err := utils.CarregarCaronas()
			if err != nil {
				log.Println("Erro ao carregar caronas:", err)
				resp := utils.MensagemResposta{Sucesso: false, Mensagem: "Erro interno no servidor."}
				respBytes, _ := json.Marshal(resp)
				conexao.Write(respBytes)
				return
			}

			// Executa a busca de rotas baseada em Grafo (BFS)
			itinerarios := utils.BuscarItinerarios(caronas, filtro.Origem, filtro.Destino, filtro.Data)

			payloadBytes, _ := json.Marshal(itinerarios)

			// Devolve o resultado formatado
			resp := utils.MensagemResposta{
				Sucesso:  true,
				Mensagem: fmt.Sprintf("Encontrado(s) %d itinerário(s) disponível(is).", len(itinerarios)),
				Payload:  payloadBytes,
			}

			respBytes, _ := json.Marshal(resp)
			conexao.Write(respBytes)

		case utils.AcaoReservarTrecho:
			var itinerarioDesejado utils.Itinerario
			if err := json.Unmarshal(req.Payload, &itinerarioDesejado); err != nil {
				log.Println("Erro ao ler payload da reserva:", err)
				resp := utils.MensagemResposta{Sucesso: false, Mensagem: "Payload de reserva inválido."}
				respBytes, _ := json.Marshal(resp)
				conexao.Write(respBytes)
				return
			}

			// Executa o processo atômico de reserva
			reserva, err := utils.ReservarItinerario(req.Usuario, itinerarioDesejado)
			if err != nil {
				log.Println("Falha na reserva:", err)
				resp := utils.MensagemResposta{
					Sucesso:  false,
					Mensagem: err.Error(),
				}
				respBytes, _ := json.Marshal(resp)
				conexao.Write(respBytes)
				return
			}

			fmt.Printf("Reserva %s confirmada para o passageiro %s!\n", reserva.ID, req.Usuario)

			payloadBytes, _ := json.Marshal(reserva)
			resp := utils.MensagemResposta{
				Sucesso:  true,
				Mensagem: "Reserva confirmada com sucesso!",
				Payload:  payloadBytes,
			}
			respBytes, _ := json.Marshal(resp)
			conexao.Write(respBytes)

		case utils.AcaoListarReservas:
			reservas, err := utils.ListarReservasPassageiro(req.Usuario)
			if err != nil {
				resp := utils.MensagemResposta{Sucesso: false, Mensagem: "Erro ao buscar reservas."}
				respBytes, _ := json.Marshal(resp)
				conexao.Write(respBytes)
				continue
			}

			payloadBytes, _ := json.Marshal(reservas)
			resp := utils.MensagemResposta{
				Sucesso:  true,
				Mensagem: fmt.Sprintf("Encontrada(s) %d reserva(s).", len(reservas)),
				Payload:  payloadBytes,
			}
			respBytes, _ := json.Marshal(resp)
			conexao.Write(respBytes)

		case utils.AcaoCancelarReserva:
			var reqCancelamento struct {
				ReservaID string `json:"reserva_id"`
			}
			if err := json.Unmarshal(req.Payload, &reqCancelamento); err != nil {
				resp := utils.MensagemResposta{Sucesso: false, Mensagem: "Payload de cancelamento inválido."}
				respBytes, _ := json.Marshal(resp)
				conexao.Write(respBytes)
				continue
			}

			err := utils.CancelarReserva(reqCancelamento.ReservaID, req.Usuario)
			if err != nil {
				resp := utils.MensagemResposta{Sucesso: false, Mensagem: err.Error()}
				respBytes, _ := json.Marshal(resp)
				conexao.Write(respBytes)
				continue
			}

			resp := utils.MensagemResposta{
				Sucesso:  true,
				Mensagem: "Reserva cancelada e assentos liberados com sucesso!",
			}
			respBytes, _ := json.Marshal(resp)
			conexao.Write(respBytes)

		case utils.AcaoConsultarCaronas:
			caronas, err := utils.ConsultarCaronasMotorista(req.Usuario)
			if err != nil {
				resp := utils.MensagemResposta{Sucesso: false, Mensagem: "Erro ao buscar caronas do motorista."}
				respBytes, _ := json.Marshal(resp)
				conexao.Write(respBytes)
				continue
			}

			payloadBytes, _ := json.Marshal(caronas)
			resp := utils.MensagemResposta{
				Sucesso:  true,
				Mensagem: fmt.Sprintf("Encontrada(s) %d carona(s) publicada(s).", len(caronas)),
				Payload:  payloadBytes,
			}
			respBytes, _ := json.Marshal(resp)
			conexao.Write(respBytes)

		case utils.AcaoCancelarCarona:
			var reqCancelamento struct {
				CaronaID  string `json:"carona_id"`
				Confirmar bool   `json:"confirmar"` // Flag enviada se o motorista deu "SIM" na confirmação
			}

			if err := json.Unmarshal(req.Payload, &reqCancelamento); err != nil {
				resp := utils.MensagemResposta{Sucesso: false, Mensagem: "Payload de cancelamento inválido."}
				respBytes, _ := json.Marshal(resp)
				conexao.Write(respBytes)
				continue
			}

			requerConfirmacao, msg, err := utils.CancelarCaronaMotorista(reqCancelamento.CaronaID, req.Usuario, reqCancelamento.Confirmar)
			if err != nil {
				resp := utils.MensagemResposta{Sucesso: false, Mensagem: err.Error()}
				respBytes, _ := json.Marshal(resp)
				conexao.Write(respBytes)
				continue
			}

			// Retorna se exige confirmação ou se o cancelamento foi finalizado
			resp := utils.MensagemResposta{
				Sucesso:  !requerConfirmacao, // Se requer confirmação, vem Sucesso: false para o cliente tratar o aviso
				Mensagem: msg,
			}
			respBytes, _ := json.Marshal(resp)
			conexao.Write(respBytes)

		case utils.AcaoConsultarPassageirosCarona:
			var reqPassageiros struct {
				CaronaID string `json:"carona_id"`
			}
			json.Unmarshal(req.Payload, &reqPassageiros)

			passageiros, err := utils.ConsultarPassageirosCarona(reqPassageiros.CaronaID, req.Usuario)
			if err != nil {
				resp := utils.MensagemResposta{Sucesso: false, Mensagem: "Erro ao consultar passageiros."}
				respBytes, _ := json.Marshal(resp)
				conexao.Write(respBytes)
				continue
			}

			payloadBytes, _ := json.Marshal(passageiros)
			resp := utils.MensagemResposta{
				Sucesso:  true,
				Mensagem: fmt.Sprintf("Passageiros na carona %s:", reqPassageiros.CaronaID),
				Payload:  payloadBytes,
			}
			respBytes, _ := json.Marshal(resp)
			conexao.Write(respBytes)

		case utils.AcaoAutenticar:
			var reqAuth struct {
				Email string            `json:"email"`
				Senha string            `json:"senha"`
				Tipo  utils.TipoUsuario `json:"tipo"`
			}

			if err := json.Unmarshal(req.Payload, &reqAuth); err != nil {
				enviarResposta(utils.MensagemResposta{Sucesso: false, Mensagem: "Payload de autenticação inválido."})
				continue
			}

			// BLOQUEIO: Verifica se já existe outra sessão ativa com este e-mail
			if !utils.RegistrarLogin(reqAuth.Email) {
				enviarResposta(utils.MensagemResposta{
					Sucesso:  false,
					Mensagem: "Acesso negado: Este usuário já está conectado em outra sessão.",
				})
				continue
			}

			sucesso, msg, err := utils.AutenticarOuCadastrarUsuario(reqAuth.Email, reqAuth.Senha, reqAuth.Tipo)
			if err != nil || !sucesso {
				// Se falhou a senha/cadastro, libera o registro que acabamos de tentar fazer
				utils.RemoverLogin(reqAuth.Email)
				enviarResposta(utils.MensagemResposta{Sucesso: false, Mensagem: msg})
				continue
			}

			// Associa o e-mail à conexão atual para liberar no fechamento
			usuarioLogadoNestaConexao = reqAuth.Email

			enviarResposta(utils.MensagemResposta{Sucesso: true, Mensagem: msg})
		}
	}
}

func main() {
	empresaID := flag.String("id", "default", "Identificador único da empresa no consórcio (ex: empA, empB, empC)")
	portaTCP := flag.String("tcp", ":8080", "Porta TCP para clientes motorista/passageiro (ex: :8080)")
	portaHTTP := flag.String("http", ":9080", "Porta HTTP da API REST do consórcio (ex: :9080)")
	urlPropria := flag.String("url", "", "URL pública HTTP desta instância para o consórcio (ex: http://localhost:9080)")
	peers := flag.String("peers", "", "Lista de URLs HTTP das outras empresas do consórcio separadas por vírgula")
	flag.Parse()

	if *urlPropria == "" {
		*urlPropria = "http://localhost" + *portaHTTP
	}

	if err := utils.ConfigurarInstancia(*empresaID, *urlPropria); err != nil {
		log.Fatalf("Erro ao configurar instância %s: %v", *empresaID, err)
	}

	// Inicia o servidor HTTP REST do consórcio, o Heartbeat e a recuperação automática de 2PC
	utils.IniciarConsorcio(*portaHTTP, *peers)

	ln, err := net.Listen("tcp", *portaTCP)
	if err != nil {
		log.Fatalln(err)
	}
	defer ln.Close()

	fmt.Printf("Servidor da empresa [%s] rodando na porta TCP %s | API REST Consórcio em %s (Peers: %s)\n",
		utils.EmpresaAtualID, *portaTCP, utils.ServidorAtualURL, *peers)

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go gerenciarConexao(conn)
	}
}
