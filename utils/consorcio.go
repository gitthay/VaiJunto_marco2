package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// PeerStatus representa o estado de conectividade de outro servidor do consórcio
type PeerStatus struct {
	URL        string    `json:"url"`
	EmpresaID  string    `json:"empresa_id"`
	Online     bool      `json:"online"`
	UltimoPing time.Time `json:"ultimo_ping"`
}

var (
	PeersConsorcio    = make(map[string]*PeerStatus)
	muPeers           sync.RWMutex
	relogioLamport    uint64 = 1
	muLamport         sync.Mutex
	muCoordenador2PC  sync.Mutex
	muSyncReservas    sync.Mutex
	transacoesMemoria []Transacao2PC // Mantém os bloqueios temporários de 2PC exclusivamente em RAM (sem arquivo extra)
	transporteHTTP    = &http.Transport{
		MaxIdleConns:        500,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     30 * time.Second,
	}
	clienteHTTP      = &http.Client{Timeout: 4000 * time.Millisecond, Transport: transporteHTTP}
	clienteHeartbeat = &http.Client{Timeout: 1500 * time.Millisecond, Transport: transporteHTTP}
)

// IncrementarLamport avança o relógio lógico local em um evento interno ou envio de mensagem
func IncrementarLamport() uint64 {
	muLamport.Lock()
	defer muLamport.Unlock()
	relogioLamport++
	return relogioLamport
}

// AtualizarLamport sincroniza o relógio lógico ao receber mensagem de outro servidor
func AtualizarLamport(recebido uint64) uint64 {
	muLamport.Lock()
	defer muLamport.Unlock()
	if recebido > relogioLamport {
		relogioLamport = recebido
	}
	relogioLamport++
	return relogioLamport
}

// CompararPrecedencia retorna true se a Transação A tem prioridade sobre a Transação B
// Critério determinístico: menor Timestamp de Lamport vence; em caso de empate, menor EmpresaID, depois menor TransacaoID
func CompararPrecedencia(lamportA uint64, empA, txA string, lamportB uint64, empB, txB string) bool {
	if lamportA != lamportB {
		return lamportA < lamportB
	}
	if empA != empB {
		return empA < empB
	}
	return txA < txB
}

// marcarReservaAbortadaEmMemoria atualiza o registro em memória quando uma reserva é cancelada
func marcarReservaAbortadaEmMemoria(reservaID string) {
	for i := range transacoesMemoria {
		if strings.TrimSpace(transacoesMemoria[i].ReservaID) == strings.TrimSpace(reservaID) {
			transacoesMemoria[i].Estado = EstadoAborted
		}
	}
}

// LimparEstado2PCMemoria reinicia a tabela de transações 2PC em memória (útil para isolamento entre testes)
func LimparEstado2PCMemoria() {
	mu.Lock()
	defer mu.Unlock()
	transacoesMemoria = nil
}

// ConfigurarPeers registra as URLs dos servidores parceiros do consórcio
func ConfigurarPeers(listaPeers string) {
	muPeers.Lock()
	defer muPeers.Unlock()

	if strings.TrimSpace(listaPeers) == "" {
		return
	}

	partes := strings.Split(listaPeers, ",")
	for _, p := range partes {
		urlLimpa := strings.TrimRight(strings.TrimSpace(p), "/")
		if urlLimpa == "" || urlLimpa == ServidorAtualURL {
			continue
		}
		PeersConsorcio[urlLimpa] = &PeerStatus{
			URL:        urlLimpa,
			Online:     true, // Assume online inicialmente até o primeiro heartbeat
			UltimoPing: time.Now(),
		}
	}
}

// IniciarConsorcio sobe o servidor HTTP REST, a rotina de Heartbeat e o coletor/recuperador de transações 2PC
func IniciarConsorcio(enderecoHTTP string, listaPeers string) {
	ConfigurarPeers(listaPeers)

	// Antes de aceitar novas operações, recupera eventuais transações pendentes e reconcilia reservas
	go func() {
		RecuperarTransacoesPendentes()
		sincronizarReservasComPeers()
	}()

	// Rotina de Batimento Cardíaco (Heartbeat), Expiração de PREPARED órfãos e Reconciliação de Reservas/Assentos
	go loopHeartbeatERecovery()

	mux := ConfigurarRotasConsorcio()
	go func() {
		if err := http.ListenAndServe(enderecoHTTP, mux); err != nil {
			RegistrarLog("[ERRO REST] Falha ao iniciar servidor HTTP do consórcio em %s: %v", enderecoHTTP, err)
		}
	}()
}

// ConfigurarRotasConsorcio cria o roteador HTTP da API REST entre servidores do consórcio
func ConfigurarRotasConsorcio() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/heartbeat", handlerHeartbeat)
	mux.HandleFunc("/api/caronas", handlerListarCaronasConsorcio)
	mux.HandleFunc("/api/caronas/detalhe", handlerDetalheCaronaConsorcio)
	mux.HandleFunc("/api/caronas/passageiros", handlerConsultarPassageirosCaronaRemota)
	mux.HandleFunc("/api/2pc/prepare", handler2PCPrepare)
	mux.HandleFunc("/api/2pc/commit", handler2PCCommit)
	mux.HandleFunc("/api/2pc/abort", handler2PCAbort)
	mux.HandleFunc("/api/2pc/status", handler2PCStatus)
	mux.HandleFunc("/api/reservas/status", handlerStatusReserva)
	mux.HandleFunc("/api/reservas/liberar", handlerLiberarAssentosReserva)
	mux.HandleFunc("/api/caronas/cancelada", handlerNotificarCaronaCancelada)
	return mux
}

// ============================================================================
// 1. BATIMENTO CARDÍACO (HEARTBEAT) E MONITORAMENTO DE SERVIDORES
// ============================================================================

func handlerHeartbeat(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":     "ONLINE",
		"empresa_id": EmpresaAtualID,
		"url":        ServidorAtualURL,
		"lamport":    IncrementarLamport(),
	})
}

func loopHeartbeatERecovery() {
	ticker := time.NewTicker(1500 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		verificarPeersHeartbeat()
		limparTransacoesPreparedExpiradas()
		sincronizarReservasComPeers()
	}
}

func verificarPeersHeartbeat() {
	muPeers.RLock()
	urls := make([]string, 0, len(PeersConsorcio))
	for u := range PeersConsorcio {
		urls = append(urls, u)
	}
	muPeers.RUnlock()

	for _, urlPeer := range urls {
		resp, err := clienteHeartbeat.Get(urlPeer + "/api/heartbeat")
		onlineAgora := err == nil && resp != nil && resp.StatusCode == http.StatusOK

		var empID string
		var lamportRemoto uint64
		if onlineAgora {
			var dados struct {
				EmpresaID string `json:"empresa_id"`
				Lamport   uint64 `json:"lamport"`
			}
			_ = json.NewDecoder(resp.Body).Decode(&dados)
			resp.Body.Close()
			empID = dados.EmpresaID
			lamportRemoto = dados.Lamport
			AtualizarLamport(lamportRemoto)
		} else if resp != nil {
			resp.Body.Close()
		}

		muPeers.Lock()
		peer, existe := PeersConsorcio[urlPeer]
		if existe {
			estavaOffline := !peer.Online
			peer.Online = onlineAgora
			if empID != "" {
				peer.EmpresaID = empID
			}
			if onlineAgora {
				peer.UltimoPing = time.Now()
				if estavaOffline {
					RegistrarLog("[HEARTBEAT] Servidor parceiro %s (%s) voltou a ficar ONLINE!", urlPeer, empID)
					go RecuperarTransacoesPendentes()
					go sincronizarCaronasInativasComPeer(urlPeer)
					go sincronizarReservasComPeers()
				}
			} else if !estavaOffline {
				RegistrarLog("[HEARTBEAT] Servidor parceiro %s ficou OFFLINE (timeout/falha detectada).", urlPeer)
			}
		}
		muPeers.Unlock()
	}
}

// ============================================================================
// 2. BUSCA DISTRIBUÍDA DE CARONAS, DETALHES SOB DEMANDA E GRAFO FEDERADO
// ============================================================================

// GET /api/caronas?dia=DD&mes=MM&ano=AAAA
func handlerListarCaronasConsorcio(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	caronas, err := CarregarCaronas()
	mu.Unlock()

	if err != nil {
		http.Error(w, "erro ao carregar caronas locais", http.StatusInternalServerError)
		return
	}

	dia, _ := strconv.Atoi(r.URL.Query().Get("dia"))
	mes, _ := strconv.Atoi(r.URL.Query().Get("mes"))
	ano, _ := strconv.Atoi(r.URL.Query().Get("ano"))
	filtrarData := dia > 0 && mes > 0 && ano > 0
	dataFiltro := NovaData(dia, mes, ano)

	var arestasDisponiveis []ArestaTrecho
	for _, c := range caronas {
		if !c.Ativa {
			continue
		}
		for _, t := range c.Trechos {
			if (!filtrarData || t.DataPartida == dataFiltro) && t.AssentosLivre > 0 {
				arestasDisponiveis = append(arestasDisponiveis, ArestaTrecho{
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
				})
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(arestasDisponiveis)
}

// GET /api/caronas/detalhe?reserva_id=RES-123&carona_id=CAR-123
// Endpoint consultado sob demanda enviando apenas o ID da reserva e o ID da carona.
// O servidor participante localiza o trecho na sua reserva/carona local e devolve todos os detalhes
// (origem, destino, motorista, horários e preço).
func handlerDetalheCaronaConsorcio(w http.ResponseWriter, r *http.Request) {
	reservaID := strings.TrimSpace(r.URL.Query().Get("reserva_id"))
	caronaID := strings.TrimSpace(r.URL.Query().Get("carona_id"))
	origem := strings.TrimSpace(r.URL.Query().Get("origem"))
	destino := strings.TrimSpace(r.URL.Query().Get("destino"))

	mu.Lock()
	caronas, err := CarregarCaronas()
	reservas, _ := CarregarReservas()
	mu.Unlock()
	if err != nil {
		http.Error(w, "erro ao ler caronas", http.StatusInternalServerError)
		return
	}

	// Se origem/destino não vieram na query, descobre pelo reserva_id + carona_id no reservas.json deste servidor
	if (origem == "" || destino == "") && reservaID != "" {
		for _, res := range reservas {
			if strings.TrimSpace(res.ID) == reservaID {
				for _, tr := range res.Itinerario {
					if strings.TrimSpace(tr.CaronaID) == caronaID && tr.Origem != "" && tr.Destino != "" {
						origem = tr.Origem
						destino = tr.Destino
						break
					}
				}
				break
			}
		}
	}

	// Fallback caso a carona seja consultada diretamente apenas pelo carona_id
	if origem == "" || destino == "" {
		for _, c := range caronas {
			if strings.TrimSpace(c.ID) == caronaID && len(c.Trechos) > 0 {
				origem = c.Trechos[0].Origem
				destino = c.Trechos[len(c.Trechos)-1].Destino
				break
			}
		}
	}

	detalhe, ok := obterDetalheTrechoEmListaCaronas(caronas, caronaID, origem, destino)
	if !ok {
		http.Error(w, "carona ou trecho não encontrado ou inativo", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(detalhe)
}

// PreencherDetalhesRemotosReserva percorre os trechos de uma reserva:
// - Se o trecho for local, garante que os dados locais estejam preenchidos.
// - Se o trecho pertencer a outro servidor participante, envia apenas reserva_id e carona_id
//   via GET /api/caronas/detalhe. Se o servidor parceiro responder, preenche origem, destino,
//   motorista, horários e preço; se estiver OFFLINE, marca tr.ServidorOffline = true.
func PreencherDetalhesRemotosReserva(r *Reserva) {
	mu.Lock()
	caronasLocais, _ := CarregarCaronas()
	mu.Unlock()

	var somaPrecos float64
	todosPrecosDisponiveis := true

	for i := range r.Itinerario {
		tr := &r.Itinerario[i]
		urlSrv := strings.TrimRight(strings.TrimSpace(tr.ServidorURL), "/")
		if urlSrv == "" || urlSrv == ServidorAtualURL {
			tr.ServidorURL = ServidorAtualURL
			if tr.EmpresaID == "" {
				tr.EmpresaID = EmpresaAtualID
			}
			if tr.MotoristaID == "" || tr.HorarioPartida.IsZero() {
				if det, ok := obterDetalheTrechoEmListaCaronas(caronasLocais, tr.CaronaID, tr.Origem, tr.Destino); ok {
					tr.Origem = det.Origem
					tr.Destino = det.Destino
					tr.MotoristaID = det.MotoristaID
					tr.HorarioPartida = det.HorarioPartida
					tr.HorarioChegada = det.HorarioChegada
					tr.Preco = det.Preco
				}
			}
			tr.ServidorOffline = false
			somaPrecos += tr.Preco
			continue
		}

		endpoint := fmt.Sprintf("%s/api/caronas/detalhe?reserva_id=%s&carona_id=%s",
			urlSrv,
			url.QueryEscape(r.ID),
			url.QueryEscape(tr.CaronaID),
		)
		resp, err := clienteHeartbeat.Get(endpoint)
		if err != nil || resp == nil {
			tr.ServidorOffline = true
			todosPrecosDisponiveis = false
			muPeers.Lock()
			if p, ok := PeersConsorcio[urlSrv]; ok {
				p.Online = false
			}
			muPeers.Unlock()
			continue
		}

		if resp.StatusCode == http.StatusOK {
			var det ArestaTrecho
			if errDec := json.NewDecoder(resp.Body).Decode(&det); errDec == nil {
				tr.Origem = det.Origem
				tr.Destino = det.Destino
				tr.MotoristaID = det.MotoristaID
				tr.HorarioPartida = det.HorarioPartida
				tr.HorarioChegada = det.HorarioChegada
				tr.Preco = det.Preco
				if tr.EmpresaID == "" && det.EmpresaID != "" {
					tr.EmpresaID = det.EmpresaID
				}
				tr.ServidorOffline = false
				somaPrecos += det.Preco
			} else {
				tr.ServidorOffline = true
				todosPrecosDisponiveis = false
			}
			resp.Body.Close()
		} else {
			resp.Body.Close()
			tr.ServidorOffline = true
			todosPrecosDisponiveis = false
		}
	}

	if todosPrecosDisponiveis && somaPrecos > 0 {
		r.ValorTotal = somaPrecos
	}
}

// BuscarItinerariosConsorcio consulta as caronas locais e todos os servidores parceiros ONLINE via REST
func BuscarItinerariosConsorcio(caronasLocais []Carona, origem, destino string, data Data) []Itinerario {
	sincronizarReservasComPeers()

	mu.Lock()
	caronasAtualizadas, err := CarregarCaronas()
	mu.Unlock()
	if err == nil {
		caronasLocais = caronasAtualizadas
	}

	grafo := MontarGrafo(caronasLocais, data)

	// Consulta cada servidor parceiro via HTTP GET /api/caronas
	muPeers.RLock()
	urls := make([]string, 0, len(PeersConsorcio))
	for u := range PeersConsorcio {
		urls = append(urls, u)
	}
	muPeers.RUnlock()

	var wg sync.WaitGroup
	var muGrafo sync.Mutex

	for _, urlPeer := range urls {
		wg.Add(1)
		go func(peerURL string) {
			defer wg.Done()
			endpoint := fmt.Sprintf("%s/api/caronas?dia=%d&mes=%d&ano=%d", peerURL, data.Dia, data.Mes, data.Ano)
			resp, err := clienteHeartbeat.Get(endpoint)
			if err != nil || resp == nil {
				// Se o parceiro falhou agora, marca como offline e segue normalmente sem travar
				muPeers.Lock()
				if p, ok := PeersConsorcio[peerURL]; ok {
					p.Online = false
				}
				muPeers.Unlock()
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				return
			}

			var arestasRemotas []ArestaTrecho
			if err := json.NewDecoder(resp.Body).Decode(&arestasRemotas); err != nil {
				return
			}

			muGrafo.Lock()
			for _, ar := range arestasRemotas {
				if ar.ServidorURL == "" {
					ar.ServidorURL = peerURL
				}
				grafo[ar.Origem] = append(grafo[ar.Origem], ar)
			}
			muGrafo.Unlock()
		}(urlPeer)
	}

	wg.Wait()
	return buscarItinerariosNoGrafo(grafo, origem, destino)
}

// executa o BFS original sobre um GrafoCidades já populado (local ou federado)
func buscarItinerariosNoGrafo(grafo GrafoCidades, origem, destino string) []Itinerario {
	origemNorm := NormalizarTexto(origem)
	destinoNorm := NormalizarTexto(destino)

	var resultados []Itinerario
	fila := [][]ArestaTrecho{}

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

		for chaveOrigem, proximasArestas := range grafo {
			if NormalizarTexto(chaveOrigem) == NormalizarTexto(ultimoTrecho.Destino) {
				for _, proxima := range proximasArestas {
					// Evita ciclos passando pela mesma cidade já visitada no caminho
					ciclo := false
					for _, passo := range caminhoAtual {
						if NormalizarTexto(passo.Origem) == NormalizarTexto(proxima.Destino) {
							ciclo = true
							break
						}
					}
					if ciclo {
						continue
					}

					mesmaCarona := proxima.CaronaID == ultimoTrecho.CaronaID && proxima.ServidorURL == ultimoTrecho.ServidorURL
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

// ============================================================================
// 3. TWO-PHASE COMMIT (2PC) COM PRECEDÊNCIA DE LAMPORT (DESCENTRALIZADO)
// ============================================================================

type Requisicao2PCPrepare struct {
	TransacaoID      string                  `json:"transacao_id"`
	ReservaID        string                  `json:"reserva_id"`
	PassageiroID     string                  `json:"passageiro_id"`
	EmpresaOrigemID  string                  `json:"empresa_origem_id"`
	CoordenadorURL   string                  `json:"coordenador_url"`
	TimestampLamport uint64                  `json:"timestamp_lamport"`
	TrechosLocais    []ReferenciaTrechoLocal `json:"trechos_locais"`
}

type Resposta2PC struct {
	Sucesso  bool            `json:"sucesso"`
	Estado   EstadoTransacao `json:"estado"`
	Mensagem string          `json:"mensagem"`
	Lamport  uint64          `json:"lamport"`
}

type Requisicao2PCDecisao struct {
	TransacaoID    string         `json:"transacao_id"`
	ReservaID      string         `json:"reserva_id"`
	PassageiroID   string         `json:"passageiro_id,omitempty"`
	CoordenadorURL string         `json:"coordenador_url,omitempty"`
	ValorTotal     float64        `json:"valor_total,omitempty"`
	Itinerario     []ArestaTrecho `json:"itinerario,omitempty"`
	Lamport        uint64         `json:"lamport"`
}

const TimeoutLock2PC = 6 * time.Second

// ExecutarPrepareLocal valida e bloqueia provisoriamente os assentos locais (Fase 1 do 2PC)
// Se o último assento estiver bloqueado em PREPARED por outra transação de MENOR prioridade
// segundo o Relógio Lógico de Lamport, preempciona a transação concorrente em memória.
func ExecutarPrepareLocal(req Requisicao2PCPrepare) (bool, string) {
	AtualizarLamport(req.TimestampLamport)

	mu.Lock()
	defer mu.Unlock()

	// Idempotência: verifica na tabela em memória
	for _, tx := range transacoesMemoria {
		if tx.TransacaoID == req.TransacaoID {
			if tx.Estado == EstadoPrepared || tx.Estado == EstadoCommitted {
				return true, "Transação já preparada/confirmada anteriormente."
			}
			if tx.Estado == EstadoAborted {
				return false, "Transação já havia sido abortada."
			}
		}
	}

	caronas, err := CarregarCaronas()
	if err != nil {
		return false, fmt.Sprintf("erro ao carregar caronas: %v", err)
	}

	// Verifica disponibilidade para todos os trechos solicitados neste servidor
	for _, trechoDesejado := range req.TrechosLocais {
		encontrouCarona := false
		for idxC, c := range caronas {
			if c.ID == trechoDesejado.CaronaID && c.Ativa {
				indices := localizarIndicesTrecho(c, trechoDesejado.Origem, trechoDesejado.Destino)
				if indices == nil {
					break
				}
				encontrouCarona = true

				for _, idx := range indices {
					if caronas[idxC].Trechos[idx].AssentosLivre <= 0 {
						// Critério de Precedência (Relógio Lógico de Lamport):
						// Verifica em memória se alguma transação ainda em PREPARED segurando assento neste trecho
						// tem MENOR prioridade (maior Timestamp de Lamport) do que a requisição atual!
						preemptou := tentarPreempcaoPorPrecedencia(&caronas, &transacoesMemoria, c.ID, idx, req)
						if !preemptou {
							return false, fmt.Sprintf("o trecho de %s para %s não possui mais assentos disponíveis", trechoDesejado.Origem, trechoDesejado.Destino)
						}
					}
				}
			}
		}
		if !encontrouCarona {
			return false, fmt.Sprintf("carona %s inativa ou trecho %s -> %s não encontrado", trechoDesejado.CaronaID, trechoDesejado.Origem, trechoDesejado.Destino)
		}
	}

	// Desconta provisoriamente os assentos (Fase PREPARED)
	for _, trechoDesejado := range req.TrechosLocais {
		for idxC, c := range caronas {
			if c.ID == trechoDesejado.CaronaID && c.Ativa {
				indices := localizarIndicesTrecho(c, trechoDesejado.Origem, trechoDesejado.Destino)
				for _, idx := range indices {
					caronas[idxC].Trechos[idx].AssentosLivre--
				}
			}
		}
	}

	dadosCaronas, _ := json.MarshalIndent(caronas, "", "  ")
	if err := os.WriteFile(ArquivoCaronas, dadosCaronas, 0644); err != nil {
		return false, "erro ao gravar bloqueio de assentos"
	}

	// Mantém o estado PREPARED apenas em memória (RAM) durante a votação do 2PC
	novaTx := Transacao2PC{
		TransacaoID:      req.TransacaoID,
		ReservaID:        req.ReservaID,
		PassageiroID:     req.PassageiroID,
		EmpresaOrigemID:  req.EmpresaOrigemID,
		CoordenadorURL:   req.CoordenadorURL,
		TimestampLamport: req.TimestampLamport,
		Estado:           EstadoPrepared,
		TrechosLocais:    req.TrechosLocais,
		CriadoEm:         time.Now(),
		ExpiraEm:         time.Now().Add(TimeoutLock2PC),
	}
	transacoesMemoria = append(transacoesMemoria, novaTx)

	RegistrarLog("[2PC PREPARE] Transação %s (Lamport=%d, Origem=%s) PREPARADA com sucesso.",
		req.TransacaoID, req.TimestampLamport, req.EmpresaOrigemID)
	return true, "Assentos pré-reservados (PREPARED)."
}

// tentarPreempcaoPorPrecedencia aplica o critério de desempate por Relógio Lógico de Lamport
// quando duas transações concorrentes disputam o último assento na fase PREPARED
func tentarPreempcaoPorPrecedencia(caronas *[]Carona, transacoes *[]Transacao2PC, caronaID string, indicePerna int, reqNova Requisicao2PCPrepare) bool {
	for i := range *transacoes {
		tx := &(*transacoes)[i]
		if tx.Estado != EstadoPrepared {
			continue
		}

		ocupaMesmaPerna := false
		for _, tr := range tx.TrechosLocais {
			if tr.CaronaID == caronaID {
				for _, c := range *caronas {
					if c.ID == caronaID {
						indicesTx := localizarIndicesTrecho(c, tr.Origem, tr.Destino)
						for _, idx := range indicesTx {
							if idx == indicePerna {
								ocupaMesmaPerna = true
								break
							}
						}
					}
				}
			}
		}

		if !ocupaMesmaPerna {
			continue
		}

		// Aplica preempção por precedência de Lamport se houver deadlock (transação presa há > 250ms)
		// ou em validação direta sem peers ativos, evitando que rajadas de 300 clientes abortem transações
		// que já estão em trânsito prestes a enviar o COMMIT!
		emPossivelDeadlockOuTeste := len(PeersConsorcio) == 0 || time.Since(tx.CriadoEm) > 250*time.Millisecond
		if emPossivelDeadlockOuTeste && reqNova.CoordenadorURL != tx.CoordenadorURL && CompararPrecedencia(
			reqNova.TimestampLamport, reqNova.EmpresaOrigemID, reqNova.TransacaoID,
			tx.TimestampLamport, tx.EmpresaOrigemID, tx.TransacaoID) {

			devolverAssentosDeTransacaoEmMemoria(caronas, *tx)
			tx.Estado = EstadoAborted
			RegistrarLog("[PRECEDENCIA LAMPORT] Transação %s (Lamport=%d, Emp=%s) venceu disputa contra %s (Lamport=%d, Emp=%s)",
				reqNova.TransacaoID, reqNova.TimestampLamport, reqNova.EmpresaOrigemID,
				tx.TransacaoID, tx.TimestampLamport, tx.EmpresaOrigemID)
			return true
		}
	}
	return false
}

func devolverAssentosDeTransacaoEmMemoria(caronas *[]Carona, tx Transacao2PC) {
	for _, trecho := range tx.TrechosLocais {
		for idxC, c := range *caronas {
			if strings.TrimSpace(c.ID) == strings.TrimSpace(trecho.CaronaID) {
				indices := localizarIndicesTrecho(c, trecho.Origem, trecho.Destino)
				for _, idx := range indices {
					if (*caronas)[idxC].Trechos[idx].AssentosLivre < (*caronas)[idxC].AssentosTot {
						(*caronas)[idxC].Trechos[idx].AssentosLivre++
					}
				}
			}
		}
	}
}

// ExecutarCommitLocal confirma definitivamente uma transação PREPARED (Fase 2 do 2PC)
// e grava no reservas.json local os detalhes dos trechos deste servidor + referências dos demais servidores participantes.
func ExecutarCommitLocal(transacaoID, reservaID string, reservaOpcional ...Reserva) (bool, string) {
	mu.Lock()
	defer mu.Unlock()

	for i, tx := range transacoesMemoria {
		if tx.TransacaoID == transacaoID {
			if tx.Estado == EstadoCommitted {
				return true, "Transação já confirmada."
			}
			if tx.Estado == EstadoAborted {
				return false, "Transação havia sido abortada por preempção ou timeout."
			}
			transacoesMemoria[i].Estado = EstadoCommitted
			if reservaID != "" {
				transacoesMemoria[i].ReservaID = reservaID
			}

			var resParaSalvar Reserva
			if len(reservaOpcional) > 0 && len(reservaOpcional[0].Itinerario) > 0 {
				resParaSalvar = reservaOpcional[0]
				if resParaSalvar.ID == "" {
					resParaSalvar.ID = transacoesMemoria[i].ReservaID
				}
				if resParaSalvar.PassageiroID == "" {
					resParaSalvar.PassageiroID = tx.PassageiroID
				}
				if resParaSalvar.CoordenadorURL == "" {
					resParaSalvar.CoordenadorURL = tx.CoordenadorURL
				}
				if resParaSalvar.DataCriacao.IsZero() {
					resParaSalvar.DataCriacao = time.Now()
				}
			} else {
				// Caso chamado na recuperação sem o payload completo, reconstrói com os trechos locais da transação
				var trechosRec []ArestaTrecho
				for _, tl := range tx.TrechosLocais {
					trechosRec = append(trechosRec, ArestaTrecho{
						CaronaID:    tl.CaronaID,
						EmpresaID:   EmpresaAtualID,
						ServidorURL: ServidorAtualURL,
						Origem:      tl.Origem,
						Destino:     tl.Destino,
					})
				}
				resParaSalvar = Reserva{
					ID:             transacoesMemoria[i].ReservaID,
					PassageiroID:   tx.PassageiroID,
					CoordenadorURL: tx.CoordenadorURL,
					Itinerario:     trechosRec,
					DataCriacao:    time.Now(),
				}
			}

			if resParaSalvar.ID != "" {
				_ = SalvarOuAtualizarReservaLocalSemLock(resParaSalvar)
			}

			RegistrarLog("[2PC COMMIT] Transação %s (Reserva %s) CONFIRMADA e gravada no reservas.json local.", transacaoID, resParaSalvar.ID)
			return true, "Transação confirmada (COMMITTED)."
		}
	}
	return false, "Transação não encontrada para COMMIT."
}

// ExecutarAbortLocal cancela uma transação PREPARED e devolve os assentos bloqueados (Fase 2 do 2PC)
func ExecutarAbortLocal(transacaoID string) (bool, string) {
	mu.Lock()
	defer mu.Unlock()

	for i, tx := range transacoesMemoria {
		if tx.TransacaoID == transacaoID {
			if tx.Estado == EstadoAborted {
				return true, "Transação já estava abortada."
			}
			if tx.Estado == EstadoPrepared || tx.Estado == EstadoCommitted {
				caronas, err := CarregarCaronas()
				if err == nil {
					devolverAssentosDeTransacaoEmMemoria(&caronas, tx)
					dadosCaronas, _ := json.MarshalIndent(caronas, "", "  ")
					_ = os.WriteFile(ArquivoCaronas, dadosCaronas, 0644)
				}
				if tx.ReservaID != "" {
					removerReservaDoArquivoSemLock(tx.ReservaID)
				}
			}
			transacoesMemoria[i].Estado = EstadoAborted
			RegistrarLog("[2PC ABORT] Transação %s ABORTADA e assentos liberados.", transacaoID)
			return true, "Transação abortada e assentos devolvidos."
		}
	}
	return true, "Transação não encontrada (nada a abortar)."
}

func removerReservaDoArquivoSemLock(reservaID string) (*Reserva, bool) {
	reservaIDClean := strings.TrimSpace(reservaID)
	if reservaIDClean == "" {
		return nil, false
	}
	reservas, err := CarregarReservas()
	if err != nil || len(reservas) == 0 {
		return nil, false
	}
	novas := make([]Reserva, 0, len(reservas))
	var removida *Reserva
	for _, r := range reservas {
		if strings.TrimSpace(r.ID) == reservaIDClean {
			copia := r
			removida = &copia
			continue
		}
		novas = append(novas, r)
	}
	if removida == nil {
		return nil, false
	}
	dados, _ := json.MarshalIndent(novas, "", "  ")
	_ = os.WriteFile(ArquivoReservas, dados, 0644)
	return removida, true
}

func handler2PCPrepare(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "método não permitido", http.StatusMethodNotAllowed)
		return
	}
	var req Requisicao2PCPrepare
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "payload inválido", http.StatusBadRequest)
		return
	}

	ok, msg := ExecutarPrepareLocal(req)
	w.Header().Set("Content-Type", "application/json")
	if !ok {
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(Resposta2PC{
			Sucesso:  false,
			Estado:   EstadoAborted,
			Mensagem: msg,
			Lamport:  IncrementarLamport(),
		})
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(Resposta2PC{
		Sucesso:  true,
		Estado:   EstadoPrepared,
		Mensagem: msg,
		Lamport:  IncrementarLamport(),
	})
}

func handler2PCCommit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "método não permitido", http.StatusMethodNotAllowed)
		return
	}
	var req Requisicao2PCDecisao
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "payload inválido", http.StatusBadRequest)
		return
	}
	AtualizarLamport(req.Lamport)

	reservaRecebida := Reserva{
		ID:             req.ReservaID,
		PassageiroID:   req.PassageiroID,
		CoordenadorURL: req.CoordenadorURL,
		Itinerario:     req.Itinerario,
		ValorTotal:     req.ValorTotal,
		DataCriacao:    time.Now(),
	}

	ok, msg := ExecutarCommitLocal(req.TransacaoID, req.ReservaID, reservaRecebida)
	w.Header().Set("Content-Type", "application/json")
	if !ok {
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(Resposta2PC{Sucesso: false, Estado: EstadoAborted, Mensagem: msg, Lamport: IncrementarLamport()})
		return
	}
	_ = json.NewEncoder(w).Encode(Resposta2PC{Sucesso: true, Estado: EstadoCommitted, Mensagem: msg, Lamport: IncrementarLamport()})
}

func handler2PCAbort(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "método não permitido", http.StatusMethodNotAllowed)
		return
	}
	var req Requisicao2PCDecisao
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "payload inválido", http.StatusBadRequest)
		return
	}
	AtualizarLamport(req.Lamport)

	ok, msg := ExecutarAbortLocal(req.TransacaoID)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(Resposta2PC{Sucesso: ok, Estado: EstadoAborted, Mensagem: msg, Lamport: IncrementarLamport()})
}

// GET /api/2pc/status?transacao_id=TX-...&reserva_id=RES-...
// Consulta o desfecho da transação diretamente na memória ou no reservas.json do coordenador
func handler2PCStatus(w http.ResponseWriter, r *http.Request) {
	txID := r.URL.Query().Get("transacao_id")
	resID := r.URL.Query().Get("reserva_id")

	mu.Lock()
	for _, tx := range transacoesMemoria {
		if tx.TransacaoID == txID {
			estado := tx.Estado
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(Resposta2PC{
				Sucesso:  true,
				Estado:   estado,
				Mensagem: "Status localizado em memória",
				Lamport:  IncrementarLamport(),
			})
			return
		}
	}

	if resID != "" {
		reservas, _ := CarregarReservas()
		for _, res := range reservas {
			if strings.TrimSpace(res.ID) == strings.TrimSpace(resID) {
				mu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(Resposta2PC{
					Sucesso:  true,
					Estado:   EstadoCommitted,
					Mensagem: "Reserva confirmada em reservas.json",
					Lamport:  IncrementarLamport(),
				})
				return
			}
		}
	}
	mu.Unlock()

	// Presumed Abort: se a reserva não foi gravada em reservas.json do coordenador, assume ABORTED
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(Resposta2PC{
		Sucesso:  true,
		Estado:   EstadoAborted,
		Mensagem: "Reserva não confirmada no coordenador (Presumed Abort)",
		Lamport:  IncrementarLamport(),
	})
}

// CoordenarReserva2PC executa o protocolo Two-Phase Commit completo via requisições REST,
// gravando de forma descentralizada a reserva em cada servidor participante.
func CoordenarReserva2PC(passageiroID string, itinerario Itinerario) (*Reserva, error) {
	muCoordenador2PC.Lock()
	defer muCoordenador2PC.Unlock()

	lamportTx := IncrementarLamport()
	agora := time.Now().UnixNano()
	transacaoID := fmt.Sprintf("TX-%s-%d", EmpresaAtualID, agora)
	reservaID := fmt.Sprintf("RES-%d", agora)

	for i := range itinerario.Trechos {
		if itinerario.Trechos[i].ServidorURL == "" {
			itinerario.Trechos[i].ServidorURL = ServidorAtualURL
		}
		if itinerario.Trechos[i].EmpresaID == "" {
			itinerario.Trechos[i].EmpresaID = EmpresaAtualID
		}
	}

	trechosPorServidor := make(map[string][]ReferenciaTrechoLocal)
	var servidoresOrdenados []string
	for _, t := range itinerario.Trechos {
		urlSrv := strings.TrimRight(t.ServidorURL, "/")
		if _, existe := trechosPorServidor[urlSrv]; !existe {
			servidoresOrdenados = append(servidoresOrdenados, urlSrv)
		}
		trechosPorServidor[urlSrv] = append(trechosPorServidor[urlSrv], ReferenciaTrechoLocal{
			CaronaID: t.CaronaID,
			Origem:   t.Origem,
			Destino:  t.Destino,
		})
	}
	sort.Strings(servidoresOrdenados)

	var servidoresPreparados []string

	// FASE 1: VOTAÇÃO (PREPARE)
	for _, urlSrv := range servidoresOrdenados {
		reqPrep := Requisicao2PCPrepare{
			TransacaoID:      transacaoID,
			ReservaID:        reservaID,
			PassageiroID:     passageiroID,
			EmpresaOrigemID:  EmpresaAtualID,
			CoordenadorURL:   ServidorAtualURL,
			TimestampLamport: lamportTx,
			TrechosLocais:    trechosPorServidor[urlSrv],
		}

		var prepOk bool
		var prepMsg string

		if urlSrv == ServidorAtualURL {
			prepOk, prepMsg = ExecutarPrepareLocal(reqPrep)
		} else {
			prepOk, prepMsg = enviarPrepareRemoto(urlSrv, reqPrep)
		}

		if !prepOk {
			// ROLLBACK ATÔMICO: Desfaz todos os servidores que já haviam preparado!
			abortarParticipantes(servidoresPreparados, transacaoID, reservaID)
			return nil, fmt.Errorf("reserva cancelada: %s", prepMsg)
		}

		servidoresPreparados = append(servidoresPreparados, urlSrv)
	}

	novaReserva := Reserva{
		ID:             reservaID,
		PassageiroID:   passageiroID,
		CoordenadorURL: ServidorAtualURL,
		Itinerario:     itinerario.Trechos,
		ValorTotal:     itinerario.PrecoTotal,
		DataCriacao:    time.Now(),
	}

	// FASE 2: CONFIRMAÇÃO (COMMIT) EM TODOS OS SERVIDORES PARTICIPANTES
	coordenadorParticipouTrecho := false
	for _, urlSrv := range servidoresPreparados {
		var commitOk bool
		var commitMsg string

		if urlSrv == ServidorAtualURL {
			coordenadorParticipouTrecho = true
			commitOk, commitMsg = ExecutarCommitLocal(transacaoID, reservaID, novaReserva)
		} else {
			commitOk, commitMsg = enviarCommitRemoto(urlSrv, transacaoID, novaReserva)
		}

		if !commitOk {
			abortarParticipantes(servidoresPreparados, transacaoID, reservaID)
			return nil, fmt.Errorf("reserva cancelada na confirmação: %s", commitMsg)
		}
	}

	// Se o passageiro estiver conectado a um servidor coordenador que não possuía trechos próprios na rota,
	// salva também a referência da reserva no servidor do passageiro para que ele possa consultá-la/cancelá-la.
	if !coordenadorParticipouTrecho {
		mu.Lock()
		err := SalvarOuAtualizarReservaLocalSemLock(novaReserva)
		mu.Unlock()
		if err != nil {
			return nil, fmt.Errorf("erro ao gravar reservas.json: %v", err)
		}
	}

	return &novaReserva, nil
}

func enviarPrepareRemoto(urlSrv string, req Requisicao2PCPrepare) (bool, string) {
	body, _ := json.Marshal(req)
	resp, err := clienteHTTP.Post(urlSrv+"/api/2pc/prepare", "application/json", bytes.NewReader(body))
	if err != nil {
		return false, fmt.Sprintf("servidor %s indisponível (%v)", urlSrv, err)
	}
	defer resp.Body.Close()

	var r2pc Resposta2PC
	_ = json.NewDecoder(resp.Body).Decode(&r2pc)
	AtualizarLamport(r2pc.Lamport)

	if resp.StatusCode != http.StatusOK || !r2pc.Sucesso {
		if r2pc.Mensagem != "" {
			return false, r2pc.Mensagem
		}
		return false, fmt.Sprintf("conflito de assentos no servidor %s", urlSrv)
	}
	return true, r2pc.Mensagem
}

func enviarCommitRemoto(urlSrv, transacaoID string, reserva Reserva) (bool, string) {
	body, _ := json.Marshal(Requisicao2PCDecisao{
		TransacaoID:    transacaoID,
		ReservaID:      reserva.ID,
		PassageiroID:   reserva.PassageiroID,
		CoordenadorURL: reserva.CoordenadorURL,
		ValorTotal:     reserva.ValorTotal,
		Itinerario:     reserva.Itinerario,
		Lamport:        IncrementarLamport(),
	})
	resp, err := clienteHTTP.Post(urlSrv+"/api/2pc/commit", "application/json", bytes.NewReader(body))
	if err != nil {
		return false, fmt.Sprintf("falha ao confirmar com servidor %s: %v", urlSrv, err)
	}
	defer resp.Body.Close()

	var r2pc Resposta2PC
	_ = json.NewDecoder(resp.Body).Decode(&r2pc)
	AtualizarLamport(r2pc.Lamport)

	return resp.StatusCode == http.StatusOK && r2pc.Sucesso, r2pc.Mensagem
}

func abortarParticipantes(servidores []string, transacaoID, reservaID string) {
	for _, urlSrv := range servidores {
		if urlSrv == ServidorAtualURL {
			ExecutarAbortLocal(transacaoID)
		} else {
			body, _ := json.Marshal(Requisicao2PCDecisao{
				TransacaoID: transacaoID,
				ReservaID:   reservaID,
				Lamport:     IncrementarLamport(),
			})
			resp, err := clienteHTTP.Post(urlSrv+"/api/2pc/abort", "application/json", bytes.NewReader(body))
			if resp != nil {
				resp.Body.Close()
			}
			_ = err
		}
	}
}

// ============================================================================
// 4. RECUPERAÇÃO DE FALHAS E RECONCILIAÇÃO DE ASSENTOS PÓS-QUEDA (VIA REST)
// ============================================================================

func limparTransacoesPreparedExpiradas() {
	mu.Lock()
	copia := append([]Transacao2PC(nil), transacoesMemoria...)
	mu.Unlock()

	agora := time.Now()
	for _, tx := range copia {
		if tx.Estado == EstadoPrepared && agora.After(tx.ExpiraEm) {
			resolverTransacaoPendente(tx)
		}
	}
}

// RecuperarTransacoesPendentes consulta via REST o coordenador das transações em PREPARED
func RecuperarTransacoesPendentes() {
	mu.Lock()
	copia := append([]Transacao2PC(nil), transacoesMemoria...)
	mu.Unlock()

	for _, tx := range copia {
		if tx.Estado == EstadoPrepared {
			resolverTransacaoPendente(tx)
		}
	}
}

func resolverTransacaoPendente(tx Transacao2PC) {
	if tx.CoordenadorURL == "" || tx.CoordenadorURL == ServidorAtualURL {
		RegistrarLog("[RECOVERY 2PC] Abortando transação local órfã %s e liberando assentos.", tx.TransacaoID)
		ExecutarAbortLocal(tx.TransacaoID)
		return
	}

	endpoint := fmt.Sprintf("%s/api/2pc/status?transacao_id=%s&reserva_id=%s", tx.CoordenadorURL, tx.TransacaoID, tx.ReservaID)
	resp, err := clienteHeartbeat.Get(endpoint)
	if err != nil || resp == nil {
		if time.Now().After(tx.ExpiraEm) {
			RegistrarLog("[RECOVERY 2PC] Coordenador %s inacessível após timeout. Liberando assentos da transação %s (Presumed Abort).",
				tx.CoordenadorURL, tx.TransacaoID)
			ExecutarAbortLocal(tx.TransacaoID)
		}
		return
	}
	defer resp.Body.Close()

	var statusResp Resposta2PC
	if err := json.NewDecoder(resp.Body).Decode(&statusResp); err != nil {
		return
	}

	if statusResp.Estado == EstadoCommitted {
		RegistrarLog("[RECOVERY 2PC] Coordenador informou COMMIT para transação %s.", tx.TransacaoID)
		ExecutarCommitLocal(tx.TransacaoID, tx.ReservaID)
	} else {
		RegistrarLog("[RECOVERY 2PC] Coordenador informou ABORT para transação %s. Liberando assentos.", tx.TransacaoID)
		ExecutarAbortLocal(tx.TransacaoID)
	}
}

// GET /api/reservas/status?reserva_id=RES-123
// Permite que um servidor que acabou de voltar de uma queda consulte os demais servidores
// participantes de uma reserva para saber se ela ainda existe ou se foi excluída/cancelada enquanto ele estava offline.
func handlerStatusReserva(w http.ResponseWriter, r *http.Request) {
	reservaID := strings.TrimSpace(r.URL.Query().Get("reserva_id"))
	if reservaID == "" {
		http.Error(w, "reserva_id obrigatório", http.StatusBadRequest)
		return
	}

	mu.Lock()
	reservas, _ := CarregarReservas()
	caronas, _ := CarregarCaronas()
	mu.Unlock()

	existe := false
	for _, res := range reservas {
		if strings.TrimSpace(res.ID) == reservaID {
			existe = true
			for _, tr := range res.Itinerario {
				urlTr := strings.TrimRight(strings.TrimSpace(tr.ServidorURL), "/")
				if urlTr == "" || urlTr == ServidorAtualURL {
					for _, c := range caronas {
						if strings.TrimSpace(c.ID) == strings.TrimSpace(tr.CaronaID) && !c.Ativa {
							existe = false
							break
						}
					}
				}
			}
			break
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"reserva_id": reservaID,
		"existe":     existe,
		"empresa_id": EmpresaAtualID,
	})
}

// sincronizarReservasComPeers reconcilia o reservas.json e o caronas.json locais com os demais servidores participantes:
// Se este servidor estava derrubado quando uma reserva da qual ele participava foi excluída (ou teve carona cancelada)
// em outro servidor, assim que voltar ou consultar os peers ele detecta que a reserva não existe mais no parceiro,
// apaga a reserva local e devolve +1 assento (AssentosLivre++) imediatamente!
func sincronizarReservasComPeers() {
	if !muSyncReservas.TryLock() {
		return
	}
	defer muSyncReservas.Unlock()

	mu.Lock()
	reservas, err := CarregarReservas()
	mu.Unlock()
	if err != nil || len(reservas) == 0 {
		return
	}

	agora := time.Now()
	for _, res := range reservas {
		// Evita checar reserva recém-criada há menos de 1.5s que ainda possa estar finalizando fase de commit
		if !res.DataCriacao.IsZero() && agora.Sub(res.DataCriacao) < 1500*time.Millisecond {
			continue
		}

		servidoresParticipantes := make(map[string]bool)
		coordURL := strings.TrimRight(strings.TrimSpace(res.CoordenadorURL), "/")
		if coordURL != "" && coordURL != ServidorAtualURL {
			servidoresParticipantes[coordURL] = true
		}
		for _, tr := range res.Itinerario {
			urlTr := strings.TrimRight(strings.TrimSpace(tr.ServidorURL), "/")
			if urlTr != "" && urlTr != ServidorAtualURL {
				servidoresParticipantes[urlTr] = true
			}
		}

		if len(servidoresParticipantes) == 0 {
			continue
		}

		canceladaNoParceiro := false
		var servidorQueCancelou string

		for urlRemota := range servidoresParticipantes {
			endpoint := fmt.Sprintf("%s/api/reservas/status?reserva_id=%s", urlRemota, url.QueryEscape(res.ID))
			resp, err := clienteHeartbeat.Get(endpoint)
			if err != nil || resp == nil {
				continue
			}
			if resp.StatusCode == http.StatusOK {
				var status struct {
					Existe bool `json:"existe"`
				}
				if errDec := json.NewDecoder(resp.Body).Decode(&status); errDec == nil && !status.Existe {
					canceladaNoParceiro = true
					servidorQueCancelou = urlRemota
				}
			}
			resp.Body.Close()
			if canceladaNoParceiro {
				break
			}
		}

		if canceladaNoParceiro {
			RegistrarLog("[RECONCILIACAO POS-QUEDA] Reserva %s foi cancelada no servidor participante %s enquanto este servidor estava offline. Removendo reserva local e restaurando +1 assento!",
				res.ID, servidorQueCancelou)
			LiberarAssentosLocais(res.ID, res.Itinerario)

			// Propaga para quaisquer outros participantes da mesma reserva caso também precisem liberar
			for urlRemota := range servidoresParticipantes {
				if urlRemota != servidorQueCancelou {
					EnviarLiberacaoRemota(urlRemota, res.ID, nil)
				}
			}
		}
	}
}

// ============================================================================
// 5. CONSULTAS E CANCELAMENTOS ENTRE EMPRESAS 100% VIA REQUISIÇÕES REST
// ============================================================================

// GET /api/caronas/passageiros?carona_id=CAR-123&servidor_url=http://...
// Chamado pelo servidor do motorista para descobrir quais passageiros desta empresa compraram vaga naquela carona
func handlerConsultarPassageirosCaronaRemota(w http.ResponseWriter, r *http.Request) {
	caronaID := strings.TrimSpace(r.URL.Query().Get("carona_id"))
	servidorOrigem := strings.TrimRight(strings.TrimSpace(r.URL.Query().Get("servidor_url")), "/")

	mu.Lock()
	reservas, _ := CarregarReservas()
	mu.Unlock()

	var passageiros []string
	for _, res := range reservas {
		for _, tr := range res.Itinerario {
			urlTrecho := strings.TrimRight(strings.TrimSpace(tr.ServidorURL), "/")
			if strings.TrimSpace(tr.CaronaID) == caronaID && (servidorOrigem == "" || urlTrecho == "" || urlTrecho == servidorOrigem) {
				passageiros = append(passageiros, res.PassageiroID)
				break
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(passageiros)
}

// ConsultarPassageirosRemotosDaCarona pergunta via HTTP GET a todos os servidores parceiros
// quais passageiros deles possuem reserva ativa na carona informada
func ConsultarPassageirosRemotosDaCarona(caronaID string) []string {
	muPeers.RLock()
	urls := make([]string, 0, len(PeersConsorcio))
	for u := range PeersConsorcio {
		urls = append(urls, u)
	}
	muPeers.RUnlock()

	var resultado []string
	var muRes sync.Mutex
	var wg sync.WaitGroup

	for _, urlPeer := range urls {
		wg.Add(1)
		go func(peerURL string) {
			defer wg.Done()
			endpoint := fmt.Sprintf("%s/api/caronas/passageiros?carona_id=%s&servidor_url=%s",
				peerURL, caronaID, ServidorAtualURL)
			resp, err := clienteHeartbeat.Get(endpoint)
			if err != nil || resp == nil {
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				return
			}
			var lista []string
			if err := json.NewDecoder(resp.Body).Decode(&lista); err == nil && len(lista) > 0 {
				muRes.Lock()
				resultado = append(resultado, lista...)
				muRes.Unlock()
			}
		}(urlPeer)
	}

	wg.Wait()
	return resultado
}

type RequisicaoLiberarTrechos struct {
	ReservaID string         `json:"reserva_id"`
	Trechos   []ArestaTrecho `json:"trechos"`
}

// POST /api/reservas/liberar -> Devolve assentos quando um passageiro de outra empresa cancela sua reserva
// ou quando outro motorista de um itinerário composto cancela sua carona
func handlerLiberarAssentosReserva(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "método não permitido", http.StatusMethodNotAllowed)
		return
	}
	var req RequisicaoLiberarTrechos
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "payload inválido", http.StatusBadRequest)
		return
	}

	LiberarAssentosLocais(req.ReservaID, req.Trechos)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"sucesso": true})
}

// LiberarAssentosLocais remove a reserva do reservas.json local e devolve +1 assento (AssentosLivre++)
// no caronas.json local de forma estritamente idempotente (nunca incrementa duas vezes a mesma reserva).
func LiberarAssentosLocais(reservaID string, trechos []ArestaTrecho) {
	mu.Lock()
	defer mu.Unlock()

	reservaIDClean := strings.TrimSpace(reservaID)
	trechosParaLiberar := trechos

	if reservaIDClean != "" {
		marcarReservaAbortadaEmMemoria(reservaIDClean)
		reservaRemovida, existiaNoArquivo := removerReservaDoArquivoSemLock(reservaIDClean)
		if !existiaNoArquivo {
			// Se a reserva já havia sido removida anteriormente deste servidor, não soma +1 novamente
			return
		}
		if reservaRemovida != nil && len(reservaRemovida.Itinerario) > 0 {
			// Usa o itinerário salvo no próprio servidor (que possui origem e destino completos dos trechos locais)
			trechosParaLiberar = reservaRemovida.Itinerario
		}
	}

	caronas, err := CarregarCaronas()
	if err == nil {
		alterouCaronas := false
		for _, tr := range trechosParaLiberar {
			urlSrv := strings.TrimRight(strings.TrimSpace(tr.ServidorURL), "/")
			if urlSrv != "" && urlSrv != ServidorAtualURL {
				continue
			}
			for idxC, c := range caronas {
				if strings.TrimSpace(c.ID) == strings.TrimSpace(tr.CaronaID) {
					var indices []int
					if tr.Origem != "" && tr.Destino != "" {
						indices = localizarIndicesTrecho(c, tr.Origem, tr.Destino)
					} else {
						for idx := range c.Trechos {
							indices = append(indices, idx)
						}
					}
					for _, idx := range indices {
						if caronas[idxC].Trechos[idx].AssentosLivre < caronas[idxC].AssentosTot {
							caronas[idxC].Trechos[idx].AssentosLivre++
							alterouCaronas = true
						}
					}
				}
			}
		}
		if alterouCaronas {
			dadosCaronas, _ := json.MarshalIndent(caronas, "", "  ")
			_ = os.WriteFile(ArquivoCaronas, dadosCaronas, 0644)
		}
	}
}

// EnviarLiberacaoRemota envia requisição REST para outro servidor devolver os assentos de uma reserva cancelada
func EnviarLiberacaoRemota(urlSrv, reservaID string, trechos []ArestaTrecho) {
	body, _ := json.Marshal(RequisicaoLiberarTrechos{
		ReservaID: reservaID,
		Trechos:   trechos,
	})
	resp, err := clienteHTTP.Post(strings.TrimRight(urlSrv, "/")+"/api/reservas/liberar", "application/json", bytes.NewReader(body))
	if resp != nil {
		resp.Body.Close()
	}
	_ = err
}

// POST /api/caronas/cancelada -> Recebe de outro servidor a notificação de que uma carona foi cancelada pelo motorista.
// Varre o reservas.json local: se encontrar reservas que usavam aquela carona, apaga-as e libera os demais trechos!
func handlerNotificarCaronaCancelada(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "método não permitido", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		ReservaID   string `json:"reserva_id,omitempty"`
		CaronaID    string `json:"carona_id"`
		ServidorURL string `json:"servidor_url,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "payload inválido", http.StatusBadRequest)
		return
	}

	caronaIDClean := strings.TrimSpace(req.CaronaID)
	srvOrigemClean := strings.TrimRight(strings.TrimSpace(req.ServidorURL), "/")

	mu.Lock()
	reservas, _ := CarregarReservas()
	novasReservas := make([]Reserva, 0, len(reservas))
	var reservasRemovidas []Reserva

	for _, res := range reservas {
		atingida := false
		if req.ReservaID != "" && strings.TrimSpace(res.ID) == strings.TrimSpace(req.ReservaID) {
			atingida = true
		} else if caronaIDClean != "" {
			for _, tr := range res.Itinerario {
				urlTr := strings.TrimRight(strings.TrimSpace(tr.ServidorURL), "/")
				if strings.TrimSpace(tr.CaronaID) == caronaIDClean && (srvOrigemClean == "" || urlTr == "" || urlTr == srvOrigemClean) {
					atingida = true
					break
				}
			}
		}

		if atingida {
			reservasRemovidas = append(reservasRemovidas, res)
			marcarReservaAbortadaEmMemoria(res.ID)
			continue
		}
		novasReservas = append(novasReservas, res)
	}

	type liberacaoPendente struct {
		url       string
		reservaID string
		trechos   []ArestaTrecho
	}
	var liberacoesRemotas []liberacaoPendente

	if len(reservasRemovidas) > 0 {
		dadosReservas, _ := json.MarshalIndent(novasReservas, "", "  ")
		_ = os.WriteFile(ArquivoReservas, dadosReservas, 0644)

		caronas, err := CarregarCaronas()
		if err == nil {
			for _, resRem := range reservasRemovidas {
				for _, tr := range resRem.Itinerario {
					if strings.TrimSpace(tr.CaronaID) == caronaIDClean {
						continue // A carona cancelada já foi desativada no servidor do motorista
					}
					urlSrv := strings.TrimRight(strings.TrimSpace(tr.ServidorURL), "/")
					if urlSrv != "" && urlSrv != ServidorAtualURL {
						liberacoesRemotas = append(liberacoesRemotas, liberacaoPendente{
							url:       urlSrv,
							reservaID: resRem.ID,
							trechos:   []ArestaTrecho{tr},
						})
						continue
					}
					for idxC, c := range caronas {
						if strings.TrimSpace(c.ID) == strings.TrimSpace(tr.CaronaID) {
							indices := localizarIndicesTrecho(c, tr.Origem, tr.Destino)
							for _, idx := range indices {
								if caronas[idxC].Trechos[idx].AssentosLivre < caronas[idxC].AssentosTot {
									caronas[idxC].Trechos[idx].AssentosLivre++
								}
							}
						}
					}
				}
			}
			dadosCaronas, _ := json.MarshalIndent(caronas, "", "  ")
			_ = os.WriteFile(ArquivoCaronas, dadosCaronas, 0644)
		}
	}
	mu.Unlock()

	for _, lib := range liberacoesRemotas {
		EnviarLiberacaoRemota(lib.url, lib.reservaID, lib.trechos)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"sucesso": true})
}

// NotificarCancelamentoCaronaAosPeers avisa via requisição REST todos os servidores parceiros
// que uma carona deste servidor foi cancelada pelo motorista
func NotificarCancelamentoCaronaAosPeers(caronaID string) {
	muPeers.RLock()
	urls := make([]string, 0, len(PeersConsorcio))
	for u := range PeersConsorcio {
		urls = append(urls, u)
	}
	muPeers.RUnlock()

	for _, urlPeer := range urls {
		NotificarCancelamentoCaronaRemota(urlPeer, "", caronaID)
	}
}

// NotificarCancelamentoCaronaRemota envia POST /api/caronas/cancelada para um servidor do consórcio
func NotificarCancelamentoCaronaRemota(destinoURL, reservaID, caronaID string) {
	body, _ := json.Marshal(map[string]string{
		"reserva_id":   reservaID,
		"carona_id":    caronaID,
		"servidor_url": ServidorAtualURL,
	})
	resp, err := clienteHTTP.Post(strings.TrimRight(destinoURL, "/")+"/api/caronas/cancelada", "application/json", bytes.NewReader(body))
	if resp != nil {
		resp.Body.Close()
	}
	_ = err
}

// sincronizarCaronasInativasComPeer é disparada quando um servidor parceiro volta a ficar ONLINE no Heartbeat:
// reenvia via REST o aviso de quaisquer caronas locais desativadas (Ativa == false) para que o parceiro
// apague reservas que tenham sido afetadas enquanto ele estava fora do ar.
func sincronizarCaronasInativasComPeer(urlPeer string) {
	mu.Lock()
	caronas, err := CarregarCaronas()
	mu.Unlock()
	if err != nil {
		return
	}

	for _, c := range caronas {
		if !c.Ativa {
			NotificarCancelamentoCaronaRemota(urlPeer, "", c.ID)
		}
	}
}
