package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"vaiJunto/utils"
)

func enviarReqSimulador(conn net.Conn, acao utils.TipoAcao, usuario string, payload interface{}) (utils.MensagemResposta, error) {
	var payloadBytes []byte
	if payload != nil {
		payloadBytes, _ = json.Marshal(payload)
	}

	req := utils.MensagemRequisicao{
		Acao:    acao,
		Usuario: usuario,
		Payload: payloadBytes,
	}

	reqBytes, _ := json.Marshal(req)
	if _, err := conn.Write(reqBytes); err != nil {
		return utils.MensagemResposta{}, err
	}

	// Lê até completar o objeto JSON (respostas grandes chegam fragmentadas em vários pacotes TCP)
	var resp utils.MensagemResposta
	err := json.NewDecoder(conn).Decode(&resp)
	return resp, err
}

// prepara as caronas nas 3 empresas para formar a rota Salvador -> Feira de Santana -> Vitória da Conquista -> Montes Claros
func publicarCaronasCenarioConsorcio(servidores []string, assentosPorCarona int, dataViagem utils.Data) {
	rotas := []struct {
		origem  string
		destino string
		hSaida  int
		hCheg   int
		preco   float64
	}{
		{"Salvador", "Feira de Santana", 8, 10, 25.0},
		{"Feira de Santana", "Vitória da Conquista", 10, 14, 65.0},
		{"Vitória da Conquista", "Montes Claros", 14, 18, 90.0},
	}

	for i, srv := range servidores {
		rota := rotas[i%len(rotas)]
		conn, err := net.DialTimeout("tcp", srv, 4*time.Second)
		if err != nil {
			log.Printf("[AVISO] Não foi possível conectar ao servidor %s para semear carona: %v\n", srv, err)
			continue
		}

		motoristaEmail := fmt.Sprintf("motorista_sim_%d_%d@consorcio.com", i+1, time.Now().UnixNano()%10000)
		_, _ = enviarReqSimulador(conn, utils.AcaoAutenticar, motoristaEmail, map[string]interface{}{
			"email": motoristaEmail,
			"senha": "123",
			"tipo":  utils.TipoMotorista,
		})

		partida := time.Date(dataViagem.Ano, time.Month(dataViagem.Mes), dataViagem.Dia, rota.hSaida, 0, 0, 0, time.Local)
		chegada := time.Date(dataViagem.Ano, time.Month(dataViagem.Mes), dataViagem.Dia, rota.hCheg, 0, 0, 0, time.Local)

		carona := utils.Carona{
			ID:          fmt.Sprintf("CAR-SIM-EMP%d-%d", i+1, time.Now().UnixNano()),
			MotoristaID: motoristaEmail,
			AssentosTot: assentosPorCarona,
			Ativa:       true,
			Trechos: []utils.Trecho{
				{
					Origem:         rota.origem,
					Destino:        rota.destino,
					DataPartida:    dataViagem,
					HorarioPartida: partida,
					HorarioChegada: chegada,
					Preco:          rota.preco,
					AssentosLivre:  assentosPorCarona,
				},
			},
		}

		resp, err := enviarReqSimulador(conn, utils.AcaoPublicarCarona, motoristaEmail, carona)
		conn.Close()
		if err == nil && resp.Sucesso {
			fmt.Printf("Carona publicada em %s: %s -> %s (%d assentos)\n",
				srv, rota.origem, rota.destino, assentosPorCarona)
		}
	}
}

func main() {
	servidoresFlag := flag.String("servers", "localhost:8081,localhost:8082,localhost:8083", "Endereços TCP dos servidores das empresas do consórcio separados por vírgula")
	qtdPassageiros := flag.Int("clientes", 30, "Quantidade de passageiros simultâneos gerando buscas e reservas")
	assentosOferta := flag.Int("assentos", 10, "Quantidade de assentos ofertados em cada trecho para o teste de carga")
	maxWorkersSimultaneos := flag.Int("fila", 45, "Tamanho máximo de conexões simultâneas na fila de disparo")
	origemBusca := flag.String("origem", "Salvador", "Cidade de origem para as buscas dos passageiros")
	destinoBusca := flag.String("destino", "Montes Claros", "Cidade de destino para as buscas dos passageiros")
	semear := flag.Bool("seed", true, "Publicar automaticamente caronas distribuídas entre as empresas antes da carga")
	flag.Parse()

	servidores := strings.Split(*servidoresFlag, ",")
	for i := range servidores {
		servidores[i] = strings.TrimSpace(servidores[i])
	}

	dataViagem := utils.NovaData(25, 11, 2026)

	fmt.Println("==================================================================")
	fmt.Println("   SIMULADOR AUTÔNOMO DE PASSAGEIROS — CONSÓRCIO VAIJUNTO (2PC)   ")
	fmt.Println("==================================================================")
	fmt.Printf("Servidores alvo : %v\n", servidores)
	fmt.Printf("Passageiros     : %d (Fila de concorrência: %d workers)\n", *qtdPassageiros, *maxWorkersSimultaneos)
	fmt.Printf("Rota buscada    : %s -> %s (Data: %s)\n", *origemBusca, *destinoBusca, dataViagem.String())
	fmt.Println("------------------------------------------------------------------")

	if *semear {
		publicarCaronasCenarioConsorcio(servidores, *assentosOferta, dataViagem)
		time.Sleep(300 * time.Millisecond)
	}

	var (
		wg                sync.WaitGroup
		buscasRealizadas  int64
		reservasSucesso   int64
		reservasRecusadas int64
		errosConexao      int64
		muLatencia        sync.Mutex
		latencias         []time.Duration
	)

	// Fila controlada por canal (semáforo) para evitar estourar o limite de sockets (SOMAXCONN) do Windows
	filaConcorrencia := make(chan struct{}, *maxWorkersSimultaneos)

	inicioSimulacao := time.Now()
	wg.Add(*qtdPassageiros)

	for i := 1; i <= *qtdPassageiros; i++ {
		idxPassageiro := i
		servidorEscolhido := servidores[(idxPassageiro-1)%len(servidores)]

		go func(id int, enderecoTCP string) {
			defer wg.Done()

			// Entra na fila antes de abrir o socket TCP
			filaConcorrencia <- struct{}{}
			defer func() { <-filaConcorrencia }()

			inicioOp := time.Now()
			conn, err := net.DialTimeout("tcp", enderecoTCP, 8*time.Second)
			if err != nil {
				fmt.Printf("[ERRO TCP DIAL] Cliente %d em %s: %v\n", id, enderecoTCP, err)
				atomic.AddInt64(&errosConexao, 1)
				return
			}
			defer conn.Close()

			email := fmt.Sprintf("sim_passageiro_%d_%d@email.com", id, time.Now().UnixNano())

			// 1. Autentica no servidor da sua empresa
			respAuth, err := enviarReqSimulador(conn, utils.AcaoAutenticar, email, map[string]interface{}{
				"email": email,
				"senha": "123",
				"tipo":  utils.TipoPassageiro,
			})
			if err != nil || !respAuth.Sucesso {
				fmt.Printf("[ERRO AUTH] Cliente %d: err=%v, resp=%+v\n", id, err, respAuth)
				atomic.AddInt64(&errosConexao, 1)
				return
			}

			// 2. Busca itinerários (acionando o grafo federado entre empresas)
			respBusca, err := enviarReqSimulador(conn, utils.AcaoBuscarItinerario, email, map[string]interface{}{
				"origem":  *origemBusca,
				"destino": *destinoBusca,
				"data":    dataViagem,
			})
			if err != nil || !respBusca.Sucesso {
				fmt.Printf("[ERRO BUSCA] Cliente %d: err=%v, resp=%+v\n", id, err, respBusca)
				atomic.AddInt64(&errosConexao, 1)
				return
			}
			atomic.AddInt64(&buscasRealizadas, 1)

			var itinerarios []utils.Itinerario
			_ = json.Unmarshal(respBusca.Payload, &itinerarios)
			if len(itinerarios) == 0 {
				atomic.AddInt64(&reservasRecusadas, 1)
				return
			}

			// Escolhe o último itinerário semeado e solicita reserva atômica (2PC)
			itinEscolhido := itinerarios[len(itinerarios)-1]
			respRes, err := enviarReqSimulador(conn, utils.AcaoReservarTrecho, email, itinEscolhido)
			duracao := time.Since(inicioOp)

			muLatencia.Lock()
			latencias = append(latencias, duracao)
			muLatencia.Unlock()

			if err != nil {
				fmt.Printf("[ERRO RESERVA 2PC] Cliente %d: %v\n", id, err)
			}

			if err == nil && respRes.Sucesso {
				atomic.AddInt64(&reservasSucesso, 1)
			} else {
				atomic.AddInt64(&reservasRecusadas, 1)
			}
		}(idxPassageiro, servidorEscolhido)
	}

	wg.Wait()
	tempoTotal := time.Since(inicioSimulacao)

	var media, min, max time.Duration
	if len(latencias) > 0 {
		min = latencias[0]
		max = latencias[0]
		var soma time.Duration
		for _, l := range latencias {
			soma += l
			if l < min {
				min = l
			}
			if l > max {
				max = l
			}
		}
		media = soma / time.Duration(len(latencias))
	}

	fmt.Println("\n==================================================================")
	fmt.Println("                 RELATÓRIO FINAL DA SIMULAÇÃO                     ")
	fmt.Println("==================================================================")
	fmt.Printf("Tempo Total de Execução           : %v\n", tempoTotal)
	fmt.Printf("Buscas Federadas Concluídas       : %d\n", buscasRealizadas)
	fmt.Printf("Reservas Confirmadas (2PC COMMIT) : %d\n", reservasSucesso)
	fmt.Printf("Reservas Recusadas (2PC ABORT)    : %d\n", reservasRecusadas)
	fmt.Printf("Erros de Conexão                  : %d\n", errosConexao)
	if len(latencias) > 0 {
		fmt.Printf("Latência Média (Busca + 2PC)      : %v (Mín: %v | Máx: %v)\n", media, min, max)
	}
	fmt.Println("==================================================================")
}
