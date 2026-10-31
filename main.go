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

// TarefaRequisicao representa uma requisição de cliente enfileirada para processamento ordenado pelos workers
type TarefaRequisicao struct {
	Req          utils.MensagemRequisicao
	ClienteAddr  string
	RespostaChan chan ResultadoRequisicao
}

// ResultadoRequisicao encapsula a resposta gerada pelo worker da fila
type ResultadoRequisicao struct {
	Resposta          utils.MensagemResposta
	NovoUsuarioLogado string
}

const (
	CapacidadeFilaRequisicoes = 2048
	TotalWorkersFila          = 16
)

var filaRequisicoes = make(chan TarefaRequisicao, CapacidadeFilaRequisicoes)

// iniciarWorkersFilaRequisicoes inicializa o pool fixo de goroutines consumidoras da fila de requisições,
// evitando que centenas de clientes simultâneos saturem o disco e as portas de rede do sistema operacional.
func iniciarWorkersFilaRequisicoes(qtdWorkers int) {
	for i := 0; i < qtdWorkers; i++ {
		go func() {
			for tarefa := range filaRequisicoes {
				res := processarRequisicaoNaFila(tarefa.Req)
				tarefa.RespostaChan <- res
			}
		}()
	}
}

func processarRequisicaoNaFila(req utils.MensagemRequisicao) ResultadoRequisicao {
	switch req.Acao {
	case utils.AcaoPublicarCarona:
		var carona utils.Carona
		if err := json.Unmarshal(req.Payload, &carona); err != nil {
			log.Println("Erro ao ler payload da carona:", err)
			return ResultadoRequisicao{
				Resposta: utils.MensagemResposta{Sucesso: false, Mensagem: "Payload da carona inválido."},
			}
		}

		if err := utils.SalvarCarona(carona); err != nil {
			log.Println("Erro ao salvar carona:", err)
			return ResultadoRequisicao{
				Resposta: utils.MensagemResposta{Sucesso: false, Mensagem: "Erro ao salvar carona no servidor."},
			}
		}

		fmt.Println("Carona cadastrada e gravada no arquivo caronas.json!")
		return ResultadoRequisicao{
			Resposta: utils.MensagemResposta{Sucesso: true, Mensagem: "Carona cadastrada com sucesso!"},
		}

	case utils.AcaoBuscarItinerario:
		var filtro struct {
			Origem  string     `json:"origem"`
			Destino string     `json:"destino"`
			Data    utils.Data `json:"data"`
		}

		if err := json.Unmarshal(req.Payload, &filtro); err != nil {
			log.Println("Erro no payload da busca:", err)
			return ResultadoRequisicao{
				Resposta: utils.MensagemResposta{Sucesso: false, Mensagem: "Payload de busca inválido."},
			}
		}

		caronas, err := utils.CarregarCaronas()
		if err != nil {
			log.Println("Erro ao carregar caronas:", err)
			return ResultadoRequisicao{
				Resposta: utils.MensagemResposta{Sucesso: false, Mensagem: "Erro interno no servidor."},
			}
		}

		itinerarios := utils.BuscarItinerarios(caronas, filtro.Origem, filtro.Destino, filtro.Data)
		payloadBytes, _ := json.Marshal(itinerarios)

		return ResultadoRequisicao{
			Resposta: utils.MensagemResposta{
				Sucesso:  true,
				Mensagem: fmt.Sprintf("Encontrado(s) %d itinerário(s) disponível(is).", len(itinerarios)),
				Payload:  payloadBytes,
			},
		}

	case utils.AcaoReservarTrecho:
		var itinerarioDesejado utils.Itinerario
		if err := json.Unmarshal(req.Payload, &itinerarioDesejado); err != nil {
			log.Println("Erro ao ler payload da reserva:", err)
			return ResultadoRequisicao{
				Resposta: utils.MensagemResposta{Sucesso: false, Mensagem: "Payload de reserva inválido."},
			}
		}

		reserva, err := utils.ReservarItinerario(req.Usuario, itinerarioDesejado)
		if err != nil {
			log.Println("Falha na reserva:", err)
			return ResultadoRequisicao{
				Resposta: utils.MensagemResposta{Sucesso: false, Mensagem: err.Error()},
			}
		}

		fmt.Printf("Reserva %s confirmada para o passageiro %s!\n", reserva.ID, req.Usuario)
		payloadBytes, _ := json.Marshal(reserva)
		return ResultadoRequisicao{
			Resposta: utils.MensagemResposta{
				Sucesso:  true,
				Mensagem: "Reserva confirmada com sucesso!",
				Payload:  payloadBytes,
			},
		}

	case utils.AcaoListarReservas:
		reservas, err := utils.ListarReservasPassageiro(req.Usuario)
		if err != nil {
			return ResultadoRequisicao{
				Resposta: utils.MensagemResposta{Sucesso: false, Mensagem: "Erro ao buscar reservas."},
			}
		}

		payloadBytes, _ := json.Marshal(reservas)
		return ResultadoRequisicao{
			Resposta: utils.MensagemResposta{
				Sucesso:  true,
				Mensagem: fmt.Sprintf("Encontrada(s) %d reserva(s).", len(reservas)),
				Payload:  payloadBytes,
			},
		}

	case utils.AcaoCancelarReserva:
		var reqCancelamento struct {
			ReservaID string `json:"reserva_id"`
		}
		if err := json.Unmarshal(req.Payload, &reqCancelamento); err != nil {
			return ResultadoRequisicao{
				Resposta: utils.MensagemResposta{Sucesso: false, Mensagem: "Payload de cancelamento inválido."},
			}
		}

		if err := utils.CancelarReserva(reqCancelamento.ReservaID, req.Usuario); err != nil {
			return ResultadoRequisicao{
				Resposta: utils.MensagemResposta{Sucesso: false, Mensagem: err.Error()},
			}
		}

		return ResultadoRequisicao{
			Resposta: utils.MensagemResposta{
				Sucesso:  true,
				Mensagem: "Reserva cancelada e assentos liberados com sucesso!",
			},
		}

	case utils.AcaoConsultarCaronas:
		caronas, err := utils.ConsultarCaronasMotorista(req.Usuario)
		if err != nil {
			return ResultadoRequisicao{
				Resposta: utils.MensagemResposta{Sucesso: false, Mensagem: "Erro ao buscar caronas do motorista."},
			}
		}

		payloadBytes, _ := json.Marshal(caronas)
		return ResultadoRequisicao{
			Resposta: utils.MensagemResposta{
				Sucesso:  true,
				Mensagem: fmt.Sprintf("Encontrada(s) %d carona(s) publicada(s).", len(caronas)),
				Payload:  payloadBytes,
			},
		}

	case utils.AcaoCancelarCarona:
		var reqCancelamento struct {
			CaronaID  string `json:"carona_id"`
			Confirmar bool   `json:"confirmar"`
		}

		if err := json.Unmarshal(req.Payload, &reqCancelamento); err != nil {
			return ResultadoRequisicao{
				Resposta: utils.MensagemResposta{Sucesso: false, Mensagem: "Payload de cancelamento inválido."},
			}
		}

		requerConfirmacao, msg, err := utils.CancelarCaronaMotorista(reqCancelamento.CaronaID, req.Usuario, reqCancelamento.Confirmar)
		if err != nil {
			return ResultadoRequisicao{
				Resposta: utils.MensagemResposta{Sucesso: false, Mensagem: err.Error()},
			}
		}

		return ResultadoRequisicao{
			Resposta: utils.MensagemResposta{
				Sucesso:  !requerConfirmacao,
				Mensagem: msg,
			},
		}

	case utils.AcaoConsultarPassageirosCarona:
		var reqPassageiros struct {
			CaronaID string `json:"carona_id"`
		}
		_ = json.Unmarshal(req.Payload, &reqPassageiros)

		passageiros, err := utils.ConsultarPassageirosCarona(reqPassageiros.CaronaID, req.Usuario)
		if err != nil {
			return ResultadoRequisicao{
				Resposta: utils.MensagemResposta{Sucesso: false, Mensagem: "Erro ao consultar passageiros."},
			}
		}

		payloadBytes, _ := json.Marshal(passageiros)
		return ResultadoRequisicao{
			Resposta: utils.MensagemResposta{
				Sucesso:  true,
				Mensagem: fmt.Sprintf("Passageiros na carona %s:", reqPassageiros.CaronaID),
				Payload:  payloadBytes,
			},
		}

	case utils.AcaoAutenticar:
		var reqAuth struct {
			Email string            `json:"email"`
			Senha string            `json:"senha"`
			Tipo  utils.TipoUsuario `json:"tipo"`
		}

		if err := json.Unmarshal(req.Payload, &reqAuth); err != nil {
			return ResultadoRequisicao{
				Resposta: utils.MensagemResposta{Sucesso: false, Mensagem: "Payload de autenticação inválido."},
			}
		}

		if !utils.RegistrarLogin(reqAuth.Email) {
			return ResultadoRequisicao{
				Resposta: utils.MensagemResposta{
					Sucesso:  false,
					Mensagem: "Acesso negado: Este usuário já está conectado em outra sessão.",
				},
			}
		}

		sucesso, msg, err := utils.AutenticarOuCadastrarUsuario(reqAuth.Email, reqAuth.Senha, reqAuth.Tipo)
		if err != nil || !sucesso {
			utils.RemoverLogin(reqAuth.Email)
			return ResultadoRequisicao{
				Resposta: utils.MensagemResposta{Sucesso: false, Mensagem: msg},
			}
		}

		return ResultadoRequisicao{
			Resposta:          utils.MensagemResposta{Sucesso: true, Mensagem: msg},
			NovoUsuarioLogado: reqAuth.Email,
		}

	default:
		return ResultadoRequisicao{
			Resposta: utils.MensagemResposta{Sucesso: false, Mensagem: "Ação desconhecida."},
		}
	}
}

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

	buf := make([]byte, 16384)
	canalResposta := make(chan ResultadoRequisicao, 1)

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

		utils.RegistrarLog("[REQUISICAO] De: %s | Usuario: %s | Acao: %s | Payload: %s",
			clienteAddr, req.Usuario, req.Acao, string(req.Payload))

		// Enfileira a requisição no canal (Fila FIFO) e aguarda o Worker processar
		filaRequisicoes <- TarefaRequisicao{
			Req:          req,
			ClienteAddr:  clienteAddr,
			RespostaChan: canalResposta,
		}

		resultado := <-canalResposta
		if resultado.NovoUsuarioLogado != "" {
			usuarioLogadoNestaConexao = resultado.NovoUsuarioLogado
		}

		respBytes, _ := json.Marshal(resultado.Resposta)
		_, _ = conexao.Write(respBytes)
		utils.RegistrarLog("[RESPOSTA] Para: %s (Usuario: %s) | Acao: %s | Sucesso: %t | Msg: %s | Payload: %s",
			clienteAddr, req.Usuario, req.Acao, resultado.Resposta.Sucesso, resultado.Resposta.Mensagem, string(resultado.Resposta.Payload))
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

	// Inicia o Pool de Workers da Fila de Requisições TCP
	iniciarWorkersFilaRequisicoes(TotalWorkersFila)

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



