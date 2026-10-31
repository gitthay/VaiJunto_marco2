package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// PeerStatus guarda o estado de conectividade (Heartbeat) de outro servidor do consórcio
type PeerStatus struct {
	URL        string    `json:"url"`
	EmpresaID  string    `json:"empresa_id"`
	Online     bool      `json:"online"`
	UltimoPing time.Time `json:"ultimo_ping"`
}

var (
	PeersConsorcio = make(map[string]*PeerStatus)
	muPeers        sync.RWMutex

	// Relógio Lógico de Lamport para ordenação/precedência distribuída sem relógios sincronizados
	relogioLamport   uint64
	muLamport        sync.Mutex
	muCoordenador2PC sync.Mutex

	clienteHTTP = &http.Client{
		Timeout: 2500 * time.Millisecond,
	}
	clienteHeartbeat = &http.Client{
		Timeout: 1200 * time.Millisecond,
	}
)

// IncrementarLamport avança o relógio lógico em um evento local/envio de mensagem
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

	// Antes de aceitar novas operações, recupera eventuais transações pendentes após queda
	go RecuperarTransacoesPendentes()

	// Rotina de Batimento Cardíaco (Heartbeat) e Expiração de PREPARED órfãos
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
	mux.HandleFunc("/api/2pc/prepare", handler2PCPrepare)
	mux.HandleFunc("/api/2pc/commit", handler2PCCommit)
	mux.HandleFunc("/api/2pc/abort", handler2PCAbort)
	mux.HandleFunc("/api/2pc/status", handler2PCStatus)
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
				}
			} else if !estavaOffline {
				RegistrarLog("[HEARTBEAT] Servidor parceiro %s ficou OFFLINE (timeout/falha detectada).", urlPeer)
			}
		}
		muPeers.Unlock()
	}
}

// ============================================================================
// 2. BUSCA DISTRIBUÍDA DE CARONAS E GRAFO FEDERADO
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

// BuscarItinerariosConsorcio consulta as caronas locais e todos os servidores parceiros ONLINE
func BuscarItinerariosConsorcio(caronasLocais []Carona, origem, destino string, data Data) []Itinerario {
	grafo := MontarGrafo(caronasLocais, data)

	// Consulta cada servidor parceiro que está ativo (ou tenta rapidamente)
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
// 3. TWO-PHASE COMMIT (2PC) COM PRECEDÊNCIA DE LAMPORT
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
	Sucesso     bool            `json:"sucesso"`
	Estado      EstadoTransacao `json:"estado"`
	Mensagem    string          `json:"mensagem"`
	Lamport     uint64          `json:"lamport"`
}

type Requisicao2PCDecisao struct {
	TransacaoID string `json:"transacao_id"`
	ReservaID   string `json:"reserva_id"`
	Lamport     uint64 `json:"lamport"`
}

const TimeoutLock2PC = 6 * time.Second

// ExecutarPrepareLocal valida e bloqueia provisoriamente os assentos locais (Fase 1 do 2PC)
// Se o último assento estiver bloqueado em PREPARED por outra transação de MENOR prioridade
// segundo o Relógio Lógico de Lamport, preempciona a transação concorrente (resolução de conflitos).
func ExecutarPrepareLocal(req Requisicao2PCPrepare) (bool, string) {
	AtualizarLamport(req.TimestampLamport)

	mu.Lock()
	defer mu.Unlock()

	transacoes, _ := CarregarTransacoesGerais()
	// Idempotência: se já está PREPARED ou COMMITTED para esta mesma transação, retorna sucesso
	for _, tx := range transacoes {
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
						// Critério de Precedência (Item 3 do PDF):
						// Verifica se existe alguma transação ainda em estado PREPARED segurando assento neste trecho
						// que tenha MENOR prioridade (maior Timestamp de Lamport) do que a requisição atual!
						preemptou := tentarPreempcaoPorPrecedencia(&caronas, &transacoes, c.ID, idx, req)
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

	// Registra no transacoesgerais.json apenas os IDs essenciais e as referências locais (sem copiar caronas de outros servidores)
	novaTx := TransacaoGeral{
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
	transacoes = append(transacoes, novaTx)
	_ = SalvarTransacoesGerais(transacoes)

	RegistrarLog("[2PC PREPARE] Transação %s (Lamport=%d, Origem=%s) PREPARADA com sucesso.",
		req.TransacaoID, req.TimestampLamport, req.EmpresaOrigemID)
	return true, "Assentos pré-reservados (PREPARED)."
}

// tentarPreempcaoPorPrecedencia aplica o critério de desempate por Relógio Lógico de Lamport
// quando duas transações concorrentes disputam o último assento na fase PREPARED
func tentarPreempcaoPorPrecedencia(caronas *[]Carona, transacoes *[]TransacaoGeral, caronaID string, indicePerna int, reqNova Requisicao2PCPrepare) bool {
	for i := range *transacoes {
		tx := &(*transacoes)[i]
		if tx.Estado != EstadoPrepared {
			continue
		}

		// Verifica se a transação em PREPARED ocupa a mesma carona e perna
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

		// Se a requisição que chegou agora pertence a OUTRO servidor e tem MAIOR precedência
		// (menor Lamport / desempate determinístico), abortamos a transação PREPARED perdedora
		// para evitar deadlock distribuído quando dois servidores distintos disputam os mesmos trechos!
		if reqNova.CoordenadorURL != tx.CoordenadorURL && CompararPrecedencia(
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

func devolverAssentosDeTransacaoEmMemoria(caronas *[]Carona, tx TransacaoGeral) {
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
func ExecutarCommitLocal(transacaoID, reservaID string) (bool, string) {
	mu.Lock()
	defer mu.Unlock()

	transacoes, _ := CarregarTransacoesGerais()
	for i, tx := range transacoes {
		if tx.TransacaoID == transacaoID {
			if tx.Estado == EstadoCommitted {
				return true, "Transação já confirmada."
			}
			if tx.Estado == EstadoAborted {
				return false, "Transação havia sido abortada por preempção ou timeout."
			}
			transacoes[i].Estado = EstadoCommitted
			if reservaID != "" {
				transacoes[i].ReservaID = reservaID
			}
			_ = SalvarTransacoesGerais(transacoes)
			RegistrarLog("[2PC COMMIT] Transação %s CONFIRMADA definitivamente.", transacaoID)
			return true, "Transação confirmada (COMMITTED)."
		}
	}
	return false, "Transação não encontrada para COMMIT."
}

// ExecutarAbortLocal cancela uma transação PREPARED e devolve os assentos bloqueados (Fase 2 do 2PC)
func ExecutarAbortLocal(transacaoID string) (bool, string) {
	mu.Lock()
	defer mu.Unlock()

	transacoes, _ := CarregarTransacoesGerais()
	for i, tx := range transacoes {
		if tx.TransacaoID == transacaoID {
			if tx.Estado == EstadoAborted {
				return true, "Transação já estava abortada."
			}
			if tx.Estado == EstadoPrepared {
				caronas, err := CarregarCaronas()
				if err == nil {
					devolverAssentosDeTransacaoEmMemoria(&caronas, tx)
					dadosCaronas, _ := json.MarshalIndent(caronas, "", "  ")
					_ = os.WriteFile(ArquivoCaronas, dadosCaronas, 0644)
				}
			}
			transacoes[i].Estado = EstadoAborted
			_ = SalvarTransacoesGerais(transacoes)
			RegistrarLog("[2PC ABORT] Transação %s ABORTADA e assentos liberados.", transacaoID)
			return true, "Transação abortada e assentos devolvidos."
		}
	}
	return true, "Transação não encontrada (nada a abortar)."
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

	ok, msg := ExecutarCommitLocal(req.TransacaoID, req.ReservaID)
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

func handler2PCStatus(w http.ResponseWriter, r *http.Request) {
	txID := r.URL.Query().Get("transacao_id")
	mu.Lock()
	transacoes, _ := CarregarTransacoesGerais()
	mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	for _, tx := range transacoes {
		if tx.TransacaoID == txID {
			_ = json.NewEncoder(w).Encode(Resposta2PC{
				Sucesso:  true,
				Estado:   tx.Estado,
				Mensagem: "Status localizado",
				Lamport:  IncrementarLamport(),
			})
			return
		}
	}
	// Se o coordenador não conhece a transação (ou caiu antes de gravar), assume ABORTED (Presumed Abort)
	_ = json.NewEncoder(w).Encode(Resposta2PC{
		Sucesso:  true,
		Estado:   EstadoAborted,
		Mensagem: "Transação não encontrada no coordenador (Presumed Abort)",
		Lamport:  IncrementarLamport(),
	})
}

// CoordenarReserva2PC executa o protocolo Two-Phase Commit completo a partir do servidor do passageiro
func CoordenarReserva2PC(passageiroID string, itinerario Itinerario) (*Reserva, error) {
	muCoordenador2PC.Lock()
	defer muCoordenador2PC.Unlock()

	lamportTx := IncrementarLamport()
	agora := time.Now().UnixNano()
	transacaoID := fmt.Sprintf("TX-%s-%d", EmpresaAtualID, agora)
	reservaID := fmt.Sprintf("RES-%d", agora)

	// Garante que todos os trechos tenham ServidorURL preenchido (se vazio, pertence a este servidor)
	for i := range itinerario.Trechos {
		if itinerario.Trechos[i].ServidorURL == "" {
			itinerario.Trechos[i].ServidorURL = ServidorAtualURL
		}
		if itinerario.Trechos[i].EmpresaID == "" {
			itinerario.Trechos[i].EmpresaID = EmpresaAtualID
		}
	}

	// Agrupa apenas as referências essenciais dos trechos por servidor participante
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
	// Ordena as URLs dos servidores em ordem alfabética para evitar Deadlock Distribuído
	sort.Strings(servidoresOrdenados)

	var servidoresPreparados []string

	// FASE 1: VOTACAO (PREPARE)
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
			registrarAbortNoCoordenador(transacaoID, reservaID, passageiroID, lamportTx, servidoresOrdenados)
			return nil, fmt.Errorf("reserva cancelada: %s", prepMsg)
		}

		servidoresPreparados = append(servidoresPreparados, urlSrv)
	}

	// FASE 2: CONFIRMAÇÃO (COMMIT)
	// Verifica se nenhum participante sofreu preempção de Lamport no meio tempo antes de finalizar
	for _, urlSrv := range servidoresPreparados {
		var commitOk bool
		var commitMsg string

		if urlSrv == ServidorAtualURL {
			commitOk, commitMsg = ExecutarCommitLocal(transacaoID, reservaID)
		} else {
			commitOk, commitMsg = enviarCommitRemoto(urlSrv, transacaoID, reservaID)
		}

		if !commitOk {
			// Caso extremo: algum participante abortou por preempção/timeout ou caiu antes do commit
			abortarParticipantes(servidoresPreparados, transacaoID, reservaID)
			registrarAbortNoCoordenador(transacaoID, reservaID, passageiroID, lamportTx, servidoresOrdenados)
			return nil, fmt.Errorf("reserva cancelada na confirmação: %s", commitMsg)
		}
	}

	// Se o coordenador não era dono de nenhum trecho da viagem, registra apenas os IDs da transação e servidores participantes
	if _, coordenadorParticipou := trechosPorServidor[ServidorAtualURL]; !coordenadorParticipou {
		mu.Lock()
		_ = RegistrarOuAtualizarTransacao(TransacaoGeral{
			TransacaoID:             transacaoID,
			ReservaID:               reservaID,
			PassageiroID:            passageiroID,
			EmpresaOrigemID:         EmpresaAtualID,
			CoordenadorURL:          ServidorAtualURL,
			ServidoresParticipantes: servidoresOrdenados,
			TimestampLamport:        lamportTx,
			Estado:                  EstadoCommitted,
			CriadoEm:                time.Now(),
			ExpiraEm:                time.Now().Add(TimeoutLock2PC),
		})
		mu.Unlock()
	}

	// Salva a reserva no reservas.json do servidor do passageiro
	novaReserva := Reserva{
		ID:           reservaID,
		PassageiroID: passageiroID,
		Itinerario:   itinerario.Trechos,
		ValorTotal:   itinerario.PrecoTotal,
		DataCriacao:  time.Now(),
	}

	mu.Lock()
	reservas, _ := CarregarReservas()
	reservas = append(reservas, novaReserva)
	dadosReservas, _ := json.MarshalIndent(reservas, "", "  ")
	err := os.WriteFile(ArquivoReservas, dadosReservas, 0644)
	mu.Unlock()

	if err != nil {
		return nil, fmt.Errorf("erro ao gravar reservas.json: %v", err)
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

func enviarCommitRemoto(urlSrv, transacaoID, reservaID string) (bool, string) {
	body, _ := json.Marshal(Requisicao2PCDecisao{
		TransacaoID: transacaoID,
		ReservaID:   reservaID,
		Lamport:     IncrementarLamport(),
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

// registrarAbortNoCoordenador garante que o coordenador registre explicitamente o estado ABORTED
// em transacoesgerais.json para responder a consultas /api/2pc/status de servidores que caíram e voltaram depois
func registrarAbortNoCoordenador(transacaoID, reservaID, passageiroID string, lamportTx uint64, servidores []string) {
	mu.Lock()
	defer mu.Unlock()

	transacoes, _ := CarregarTransacoesGerais()
	for i, tx := range transacoes {
		if tx.TransacaoID == transacaoID {
			transacoes[i].Estado = EstadoAborted
			_ = SalvarTransacoesGerais(transacoes)
			return
		}
	}

	transacoes = append(transacoes, TransacaoGeral{
		TransacaoID:             transacaoID,
		ReservaID:               reservaID,
		PassageiroID:            passageiroID,
		EmpresaOrigemID:         EmpresaAtualID,
		CoordenadorURL:          ServidorAtualURL,
		ServidoresParticipantes: servidores,
		TimestampLamport:        lamportTx,
		Estado:                  EstadoAborted,
		CriadoEm:                time.Now(),
		ExpiraEm:                time.Now().Add(TimeoutLock2PC),
	})
	_ = SalvarTransacoesGerais(transacoes)
}

// ============================================================================
// 4. RECUPERAÇÃO DE FALHAS E TIMEOUT DE ASSENTOS BLOQUEADOS
// ============================================================================

// limparTransacoesPreparedExpiradas evita que a queda do coordenador no meio do 2PC
// deixe assentos bloqueados indefinidamente nos participantes (Item 4 do PDF)
func limparTransacoesPreparedExpiradas() {
	mu.Lock()
	transacoes, err := CarregarTransacoesGerais()
	mu.Unlock()
	if err != nil || len(transacoes) == 0 {
		return
	}

	agora := time.Now()
	for _, tx := range transacoes {
		if tx.Estado == EstadoPrepared && agora.After(tx.ExpiraEm) {
			resolverTransacaoPendente(tx)
		}
	}
}

// RecuperarTransacoesPendentes é executada quando o servidor reinicia após uma queda
// ou quando um parceiro volta a ficar ONLINE
func RecuperarTransacoesPendentes() {
	mu.Lock()
	transacoes, err := CarregarTransacoesGerais()
	mu.Unlock()
	if err != nil || len(transacoes) == 0 {
		return
	}

	for _, tx := range transacoes {
		if tx.Estado == EstadoPrepared {
			resolverTransacaoPendente(tx)
		}
	}
}

func resolverTransacaoPendente(tx TransacaoGeral) {
	// Se este próprio servidor era o coordenador e ainda está em PREPARED sem ter chegado ao COMMIT, aborta!
	if tx.CoordenadorURL == "" || tx.CoordenadorURL == ServidorAtualURL {
		RegistrarLog("[RECOVERY 2PC] Abortando transação local órfã %s e liberando assentos.", tx.TransacaoID)
		ExecutarAbortLocal(tx.TransacaoID)
		return
	}

	// Pergunta ao servidor coordenador qual foi o desfecho da transação
	resp, err := clienteHeartbeat.Get(tx.CoordenadorURL + "/api/2pc/status?transacao_id=" + tx.TransacaoID)
	if err != nil || resp == nil {
		// Se já expirou o tempo limite e o coordenador está inacessível, aplica Presumed Abort
		// para nunca manter assento bloqueado indefinidamente!
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

// ============================================================================
// 5. CANCELAMENTOS ENTRE EMPRESAS (RESERVAS E CARONAS)
// ============================================================================

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

// LiberarAssentosLocais devolve os assentos no caronas.json local e marca a transação em transacoesgerais.json como ABORTED
func LiberarAssentosLocais(reservaID string, trechos []ArestaTrecho) {
	mu.Lock()
	defer mu.Unlock()

	caronas, err := CarregarCaronas()
	if err == nil {
		for _, tr := range trechos {
			if tr.ServidorURL != "" && tr.ServidorURL != ServidorAtualURL {
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
		dadosCaronas, _ := json.MarshalIndent(caronas, "", "  ")
		_ = os.WriteFile(ArquivoCaronas, dadosCaronas, 0644)
	}

	if reservaID != "" {
		transacoes, _ := CarregarTransacoesGerais()
		alterou := false
		for i, tx := range transacoes {
			if strings.TrimSpace(tx.ReservaID) == strings.TrimSpace(reservaID) && tx.Estado != EstadoAborted {
				transacoes[i].Estado = EstadoAborted
				alterou = true
			}
		}
		if alterou {
			_ = SalvarTransacoesGerais(transacoes)
		}
	}
}

// EnviarLiberacaoRemota envia requisição para outro servidor devolver os assentos de uma reserva cancelada
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

// POST /api/caronas/cancelada -> Notifica o servidor do passageiro (coordenador) que uma carona remota foi cancelada pelo motorista.
// O servidor do passageiro remove a reserva e envia requisição para liberar os assentos nos demais servidores participantes!
func handlerNotificarCaronaCancelada(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "método não permitido", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		ReservaID string `json:"reserva_id"`
		CaronaID  string `json:"carona_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "payload inválido", http.StatusBadRequest)
		return
	}

	mu.Lock()
	reservas, _ := CarregarReservas()
	novasReservas := make([]Reserva, 0, len(reservas))
	var reservaRemovida *Reserva

	for _, res := range reservas {
		if strings.TrimSpace(res.ID) == strings.TrimSpace(req.ReservaID) {
			copia := res
			reservaRemovida = &copia
			continue
		}
		novasReservas = append(novasReservas, res)
	}
	dadosReservas, _ := json.MarshalIndent(novasReservas, "", "  ")
	_ = os.WriteFile(ArquivoReservas, dadosReservas, 0644)

	// Se a reserva tinha trechos locais neste próprio servidor ou em outros servidores do consórcio, prepara a liberação
	trechosRemotosPorServidor := make(map[string][]ArestaTrecho)
	if reservaRemovida != nil {
		caronas, err := CarregarCaronas()
		if err == nil {
			for _, tr := range reservaRemovida.Itinerario {
				if tr.CaronaID == req.CaronaID {
					continue // A carona cancelada já liberou seus assentos lá na origem
				}
				urlSrv := strings.TrimRight(strings.TrimSpace(tr.ServidorURL), "/")
				if urlSrv != "" && urlSrv != ServidorAtualURL {
					trechosRemotosPorServidor[urlSrv] = append(trechosRemotosPorServidor[urlSrv], tr)
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
			dadosCaronas, _ := json.MarshalIndent(caronas, "", "  ")
			_ = os.WriteFile(ArquivoCaronas, dadosCaronas, 0644)
		}
	}

	transacoes, _ := CarregarTransacoesGerais()
	for i, tx := range transacoes {
		if strings.TrimSpace(tx.ReservaID) == strings.TrimSpace(req.ReservaID) {
			transacoes[i].Estado = EstadoAborted
		}
	}
	_ = SalvarTransacoesGerais(transacoes)
	mu.Unlock()

	// Pede via requisição REST para que os outros servidores do itinerário devolvam os assentos
	for urlSrv, trechos := range trechosRemotosPorServidor {
		EnviarLiberacaoRemota(urlSrv, req.ReservaID, trechos)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"sucesso": true})
}

// NotificarCancelamentoCaronaRemota avisa o coordenador (empresa do passageiro) para remover a reserva de reservas.json
func NotificarCancelamentoCaronaRemota(coordenadorURL, reservaID, caronaID string) {
	body, _ := json.Marshal(map[string]string{
		"reserva_id": reservaID,
		"carona_id":  caronaID,
	})
	resp, err := clienteHTTP.Post(strings.TrimRight(coordenadorURL, "/")+"/api/caronas/cancelada", "application/json", bytes.NewReader(body))
	if resp != nil {
		resp.Body.Close()
	}
	_ = err
}

