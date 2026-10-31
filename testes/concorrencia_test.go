package testes

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"vaiJunto/utils"
)

// configurarAmbienteTesteIsolado cria um diretório temporário isolado para os 3 arquivos originais
// (caronas.json, reservas.json, usuarios.json) e limpa o estado em memória ao finalizar o teste.
func configurarAmbienteTesteIsolado(t *testing.T, empresaID string) func() {
	pastaTemp, err := os.MkdirTemp("", "vaijunto_test_"+empresaID+"_*")
	if err != nil {
		t.Fatalf("Erro ao criar diretório temporário: %v", err)
	}

	origCaronas := utils.ArquivoCaronas
	origReservas := utils.ArquivoReservas
	origUsuarios := utils.ArquivoUsuarios
	origLog := utils.ArquivoLog
	origEmp := utils.EmpresaAtualID
	origURL := utils.ServidorAtualURL

	utils.ArquivoCaronas = pastaTemp + "/caronas.json"
	utils.ArquivoReservas = pastaTemp + "/reservas.json"
	utils.ArquivoUsuarios = pastaTemp + "/usuarios.json"
	utils.ArquivoLog = pastaTemp + "/servidor.log"
	utils.EmpresaAtualID = empresaID
	utils.PeersConsorcio = make(map[string]*utils.PeerStatus)
	utils.LimparEstado2PCMemoria()

	return func() {
		utils.ArquivoCaronas = origCaronas
		utils.ArquivoReservas = origReservas
		utils.ArquivoUsuarios = origUsuarios
		utils.ArquivoLog = origLog
		utils.EmpresaAtualID = origEmp
		utils.ServidorAtualURL = origURL
		utils.PeersConsorcio = make(map[string]*utils.PeerStatus)
		utils.LimparEstado2PCMemoria()
		_ = os.RemoveAll(pastaTemp)
	}
}

// ============================================================================
// TESTE 1: Heartbeat (Monitoramento ONLINE/OFFLINE) e Relógio Lógico de Lamport
// ============================================================================
func Test1_HeartbeatESincronizacaoLamport(t *testing.T) {
	cleanup := configurarAmbienteTesteIsolado(t, "empresaA")
	defer cleanup()

	srvA := httptest.NewServer(utils.ConfigurarRotasConsorcio())
	defer srvA.Close()
	utils.ServidorAtualURL = srvA.URL

	// Verifica resposta do endpoint GET /api/heartbeat
	resp, err := http.Get(srvA.URL + "/api/heartbeat")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("Falha no endpoint /api/heartbeat: %v", err)
	}
	defer resp.Body.Close()

	var hb struct {
		Status    string `json:"status"`
		EmpresaID string `json:"empresa_id"`
		Lamport   uint64 `json:"lamport"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&hb); err != nil {
		t.Fatalf("Erro ao decodificar JSON do heartbeat: %v", err)
	}
	if hb.Status != "ONLINE" || hb.EmpresaID != "empresaA" {
		t.Fatalf("Resposta inesperada no heartbeat: %+v", hb)
	}

	// Testa avanço e sincronização do Relógio Lógico de Lamport ao receber timestamp maior
	lamportAposSync := utils.AtualizarLamport(500)
	if lamportAposSync <= 500 {
		t.Fatalf("Relógio de Lamport deveria avançar além de 500, mas ficou em %d", lamportAposSync)
	}

	t.Logf("[OK TESTE 1]: Heartbeat ONLINE e sincronização do Relógio de Lamport (Lamport=%d) validados!", lamportAposSync)
}

// ============================================================================
// TESTE 2: Busca Federada no Grafo do Consórcio (3 Empresas) e Queda de Nó
// ============================================================================
func Test2_BuscaFederadaGrafoConsorcio(t *testing.T) {
	cleanup := configurarAmbienteTesteIsolado(t, "empresaA")
	defer cleanup()

	dataViagem := utils.NovaData(20, 11, 2026)
	tBase := time.Date(2026, 11, 20, 8, 0, 0, 0, time.Local)

	// Trecho 1 (Local na Empresa A): Salvador -> Feira de Santana (08:00 às 09:30)
	caronaA := utils.Carona{
		ID:          "CAR-A-1",
		MotoristaID: "motA@email.com",
		AssentosTot: 3,
		Ativa:       true,
		Trechos: []utils.Trecho{{
			Origem:         "Salvador",
			Destino:        "Feira de Santana",
			DataPartida:    dataViagem,
			HorarioPartida: tBase,
			HorarioChegada: tBase.Add(90 * time.Minute),
			Preco:          25.0,
			AssentosLivre:  3,
		}},
	}
	_ = utils.SalvarCarona(caronaA)

	// Servidor parceiro Empresa B: Feira de Santana -> Vitória da Conquista (10:00 às 14:00)
	srvB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		trechosB := []utils.ArestaTrecho{{
			CaronaID:       "CAR-B-1",
			MotoristaID:    "motB@email.com",
			EmpresaID:      "empresaB",
			Origem:         "Feira de Santana",
			Destino:        "Vitória da Conquista",
			HorarioPartida: tBase.Add(2 * time.Hour),
			HorarioChegada: tBase.Add(6 * time.Hour),
			Preco:          65.0,
			AssentosLivre:  2,
		}}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(trechosB)
	}))
	defer srvB.Close()

	// Servidor parceiro Empresa C: Vitória da Conquista -> Montes Claros (15:00 às 19:00)
	var muC sync.Mutex
	empresaCOnline := true
	srvC := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		muC.Lock()
		ativa := empresaCOnline
		muC.Unlock()
		if !ativa {
			http.Error(w, "Empresa C fora do ar", http.StatusServiceUnavailable)
			return
		}
		trechosC := []utils.ArestaTrecho{{
			CaronaID:       "CAR-C-1",
			MotoristaID:    "motC@email.com",
			EmpresaID:      "empresaC",
			Origem:         "Vitória da Conquista",
			Destino:        "Montes Claros",
			HorarioPartida: tBase.Add(7 * time.Hour),
			HorarioChegada: tBase.Add(11 * time.Hour),
			Preco:          80.0,
			AssentosLivre:  1,
		}}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(trechosC)
	}))
	defer srvC.Close()

	utils.ConfigurarPeers(srvB.URL + "," + srvC.URL)
	caronasLocais, _ := utils.CarregarCaronas()

	// 1. Com todas as 3 empresas ONLINE, deve montar o itinerário completo Salvador -> Montes Claros (A + B + C)
	itinerarios := utils.BuscarItinerarios(caronasLocais, "Salvador", "Montes Claros", dataViagem)
	if len(itinerarios) != 1 || len(itinerarios[0].Trechos) != 3 {
		t.Fatalf("Esperava 1 itinerário com 3 trechos (A+B+C), mas obteve: %+v", itinerarios)
	}
	if itinerarios[0].PrecoTotal != 170.0 {
		t.Fatalf("Preço total esperado 170.0, obteve %.2f", itinerarios[0].PrecoTotal)
	}

	// 2. Derruba a Empresa C: a busca não pode travar e deve continuar achando Salvador -> Vitória da Conquista (A + B)
	muC.Lock()
	empresaCOnline = false
	muC.Unlock()

	itinParcial := utils.BuscarItinerarios(caronasLocais, "Salvador", "Vitória da Conquista", dataViagem)
	if len(itinParcial) != 1 || len(itinParcial[0].Trechos) != 2 {
		t.Fatalf("Com a Empresa C offline, deveria continuar encontrando a rota entre Empresa A e Empresa B!")
	}

	t.Log("[OK TESTE 2]: Busca federada no grafo (A + B + C) e tolerância à queda de servidor na busca validadas!")
}

// ============================================================================
// TESTE 3: Concorrência Sob Carga no 2PC (25 Passageiros Disputando 1 Assento)
// ============================================================================
func Test3_Concorrencia2PCDisputaUltimoAssentoInterEmpresas(t *testing.T) {
	cleanup := configurarAmbienteTesteIsolado(t, "empresaA")
	defer cleanup()

	srvA := httptest.NewServer(utils.ConfigurarRotasConsorcio())
	defer srvA.Close()
	utils.ServidorAtualURL = srvA.URL

	// Empresa A tem 25 assentos no Trecho 1, mas Empresa B tem APENAS 1 assento no Trecho 2!
	caronaA := utils.Carona{
		ID:          "CAR-EMP-A-CARGA",
		MotoristaID: "motA@email.com",
		AssentosTot: 25,
		Ativa:       true,
		Trechos: []utils.Trecho{{
			Origem:         "Salvador",
			Destino:        "Feira de Santana",
			DataPartida:    utils.NovaData(20, 11, 2026),
			HorarioPartida: time.Now(),
			HorarioChegada: time.Now().Add(time.Hour),
			Preco:          25.0,
			AssentosLivre:  25,
		}},
	}
	_ = utils.SalvarCarona(caronaA)

	var muB sync.Mutex
	assentosB := 1
	transacoesB := make(map[string]utils.EstadoTransacao)

	muxB := http.NewServeMux()
	muxB.HandleFunc("/api/2pc/prepare", func(w http.ResponseWriter, r *http.Request) {
		muB.Lock()
		defer muB.Unlock()
		var req utils.Requisicao2PCPrepare
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "application/json")
		if assentosB <= 0 {
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(utils.Resposta2PC{
				Sucesso:  false,
				Estado:   utils.EstadoAborted,
				Mensagem: "Sem assentos na Empresa B",
				Lamport:  req.TimestampLamport + 1,
			})
			return
		}
		assentosB--
		transacoesB[req.TransacaoID] = utils.EstadoPrepared
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(utils.Resposta2PC{
			Sucesso: true,
			Estado:  utils.EstadoPrepared,
			Lamport: req.TimestampLamport + 1,
		})
	})
	muxB.HandleFunc("/api/2pc/commit", func(w http.ResponseWriter, r *http.Request) {
		muB.Lock()
		defer muB.Unlock()
		var req utils.Requisicao2PCDecisao
		_ = json.NewDecoder(r.Body).Decode(&req)
		transacoesB[req.TransacaoID] = utils.EstadoCommitted
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(utils.Resposta2PC{Sucesso: true, Estado: utils.EstadoCommitted})
	})
	muxB.HandleFunc("/api/2pc/abort", func(w http.ResponseWriter, r *http.Request) {
		muB.Lock()
		defer muB.Unlock()
		var req utils.Requisicao2PCDecisao
		_ = json.NewDecoder(r.Body).Decode(&req)
		if transacoesB[req.TransacaoID] == utils.EstadoPrepared {
			assentosB++
		}
		transacoesB[req.TransacaoID] = utils.EstadoAborted
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(utils.Resposta2PC{Sucesso: true, Estado: utils.EstadoAborted})
	})
	srvB := httptest.NewServer(muxB)
	defer srvB.Close()

	itinComposto := utils.Itinerario{
		PrecoTotal: 90.0,
		Trechos: []utils.ArestaTrecho{
			{CaronaID: "CAR-EMP-A-CARGA", EmpresaID: "empresaA", ServidorURL: srvA.URL, Origem: "Salvador", Destino: "Feira de Santana", Preco: 25.0},
			{CaronaID: "CAR-EMP-B-UNICO", EmpresaID: "empresaB", ServidorURL: srvB.URL, Origem: "Feira de Santana", Destino: "Vitória da Conquista", Preco: 65.0},
		},
	}

	const totalConcorrentes = 25
	var wg sync.WaitGroup
	var muCont sync.Mutex
	confirmados := 0
	rejeitados := 0

	wg.Add(totalConcorrentes)
	for i := 0; i < totalConcorrentes; i++ {
		email := fmt.Sprintf("passageiro_%d@email.com", i)
		go func(p string) {
			defer wg.Done()
			_, err := utils.CoordenarReserva2PC(p, itinComposto)
			muCont.Lock()
			if err == nil {
				confirmados++
			} else {
				rejeitados++
			}
			muCont.Unlock()
		}(email)
	}
	wg.Wait()

	if confirmados != 1 || rejeitados != totalConcorrentes-1 {
		t.Fatalf("Falha de atomicidade sob carga! Esperava 1 confirmado e %d rejeitados, obteve %d confirmados e %d rejeitados",
			totalConcorrentes-1, confirmados, rejeitados)
	}

	// Verifica se os 24 passageiros rejeitados na Empresa B devolveram seus assentos na Empresa A (25 - 1 = 24 livres)
	caronasFinais, _ := utils.CarregarCaronas()
	if caronasFinais[0].Trechos[0].AssentosLivre != 24 {
		t.Fatalf("Assentos não devolvidos na Empresa A para os 24 rejeitados! Esperava 24 livres, obteve %d",
			caronasFinais[0].Trechos[0].AssentosLivre)
	}

	t.Logf("[OK TESTE 3]: 25 passageiros simultâneos -> 1 aprovado, 24 sofreram rollback atômico na Empresa A (AssentosLivre=24)!")
}

// ============================================================================
// TESTE 4: Injeção de Falha — Queda de Participante no Meio da Reserva (Rollback)
// ============================================================================
func Test4_InjecaoDeFalhaParticipanteNoMeioDaReserva(t *testing.T) {
	cleanup := configurarAmbienteTesteIsolado(t, "empresaA")
	defer cleanup()

	srvA := httptest.NewServer(utils.ConfigurarRotasConsorcio())
	defer srvA.Close()
	utils.ServidorAtualURL = srvA.URL

	caronaA := utils.Carona{
		ID:          "CAR-A-FALHA",
		MotoristaID: "motA@email.com",
		AssentosTot: 1,
		Ativa:       true,
		Trechos: []utils.Trecho{{
			Origem:         "Salvador",
			Destino:        "Feira de Santana",
			DataPartida:    utils.NovaData(20, 11, 2026),
			HorarioPartida: time.Now(),
			HorarioChegada: time.Now().Add(time.Hour),
			Preco:          25.0,
			AssentosLivre:  1,
		}},
	}
	_ = utils.SalvarCarona(caronaA)

	// Servidor da Empresa B cai exatamente antes do PREPARE do 2º trecho
	srvB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Servidor Empresa B caiu abruptamente", http.StatusServiceUnavailable)
	}))
	srvB.Close() // Fecha o servidor para simular queda real de conexão TCP/HTTP!

	itin := utils.Itinerario{
		PrecoTotal: 90.0,
		Trechos: []utils.ArestaTrecho{
			{CaronaID: "CAR-A-FALHA", EmpresaID: "empresaA", ServidorURL: srvA.URL, Origem: "Salvador", Destino: "Feira de Santana", Preco: 25.0},
			{CaronaID: "CAR-B-CAIU", EmpresaID: "empresaB", ServidorURL: srvB.URL, Origem: "Feira de Santana", Destino: "Alagoinhas", Preco: 65.0},
		},
	}

	_, err := utils.CoordenarReserva2PC("passageiro_queda@email.com", itin)
	if err == nil {
		t.Fatalf("Reserva deveria ter sido abortada pois a Empresa B está fora do ar!")
	}

	// Confirma que nenhuma reserva foi salva pela metade e que o assento da Empresa A voltou para 1
	reservas, _ := utils.CarregarReservas()
	if len(reservas) != 0 {
		t.Fatalf("Nenhuma reserva deveria existir após falha no 2PC, mas encontrou %d", len(reservas))
	}
	caronas, _ := utils.CarregarCaronas()
	if caronas[0].Trechos[0].AssentosLivre != 1 {
		t.Fatalf("Assento da Empresa A ficou preso após falha da Empresa B! AssentosLivre=%d", caronas[0].Trechos[0].AssentosLivre)
	}

	t.Log("[OK TESTE 4]: Queda real do servidor participante durante o 2PC desfez imediatamente a pré-reserva local!")
}

// ============================================================================
// TESTE 5: Queda do Coordenador após PREPARE e Recuperação Automática via REST
// ============================================================================
func Test5_QuedaCoordenadorERecuperacaoAutomaticaViaREST(t *testing.T) {
	cleanup := configurarAmbienteTesteIsolado(t, "empresaA")
	defer cleanup()

	srvA := httptest.NewServer(utils.ConfigurarRotasConsorcio())
	defer srvA.Close()
	utils.ServidorAtualURL = srvA.URL

	caronaA := utils.Carona{
		ID:          "CAR-A-RECOVERY",
		MotoristaID: "motA@email.com",
		AssentosTot: 1,
		Ativa:       true,
		Trechos: []utils.Trecho{{
			Origem:         "Salvador",
			Destino:        "Feira de Santana",
			DataPartida:    utils.NovaData(20, 11, 2026),
			HorarioPartida: time.Now(),
			HorarioChegada: time.Now().Add(time.Hour),
			Preco:          25.0,
			AssentosLivre:  1,
		}},
	}
	_ = utils.SalvarCarona(caronaA)

	// Coordenador remoto (Empresa C) que caiu antes de gravar a reserva no seu reservas.json
	coordenadorC := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(utils.Resposta2PC{
			Sucesso:  true,
			Estado:   utils.EstadoAborted,
			Mensagem: "Reserva não confirmada no coordenador (Presumed Abort)",
		})
	}))
	defer coordenadorC.Close()

	// Coordenador C envia PREPARE bloqueando o único assento da Empresa A e "morre" antes de enviar COMMIT/ABORT
	reqPrep := utils.Requisicao2PCPrepare{
		TransacaoID:      "TX-ORFA-500",
		ReservaID:        "RES-ORFA-500",
		PassageiroID:     "orfao@email.com",
		EmpresaOrigemID:  "empresaC",
		CoordenadorURL:   coordenadorC.URL,
		TimestampLamport: 200,
		TrechosLocais: []utils.ReferenciaTrechoLocal{{
			CaronaID: "CAR-A-RECOVERY",
			Origem:   "Salvador",
			Destino:  "Feira de Santana",
		}},
	}
	body, _ := json.Marshal(reqPrep)
	resp, err := http.Post(srvA.URL+"/api/2pc/prepare", "application/json", bytes.NewReader(body))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("Erro no PREPARE inicial: %v", err)
	}
	resp.Body.Close()

	// Verifica que o assento está temporariamente bloqueado (AssentosLivre == 0)
	caronasAntes, _ := utils.CarregarCaronas()
	if caronasAntes[0].Trechos[0].AssentosLivre != 0 {
		t.Fatalf("Assento deveria estar bloqueado (0) durante o PREPARED")
	}

	// Dispara a recuperação via REST (/api/2pc/status)
	utils.RecuperarTransacoesPendentes()

	// Verifica que o assento foi desbloqueado automaticamente (AssentosLivre == 1)
	caronasDepois, _ := utils.CarregarCaronas()
	if caronasDepois[0].Trechos[0].AssentosLivre != 1 {
		t.Fatalf("O assento continuou preso após a recuperação via REST! AssentosLivre=%d", caronasDepois[0].Trechos[0].AssentosLivre)
	}

	t.Log("[OK TESTE 5]: Recuperação automática pós-queda de coordenador via GET /api/2pc/status liberou o assento bloqueado!")
}

// ============================================================================
// TESTE 6: Precedência por Relógio Lógico de Lamport e Desempate Determinístico
// ============================================================================
func Test6_PrecedenciaRelogioLogicoLamportEDesempate(t *testing.T) {
	cleanup := configurarAmbienteTesteIsolado(t, "empresaC")
	defer cleanup()

	caronaC := utils.Carona{
		ID:          "CAR-C-DISPUTA",
		MotoristaID: "motC@email.com",
		AssentosTot: 1,
		Ativa:       true,
		Trechos: []utils.Trecho{{
			Origem:         "Vitória da Conquista",
			Destino:        "Montes Claros",
			DataPartida:    utils.NovaData(20, 11, 2026),
			HorarioPartida: time.Now(),
			HorarioChegada: time.Now().Add(time.Hour),
			Preco:          80.0,
			AssentosLivre:  1,
		}},
	}
	_ = utils.SalvarCarona(caronaC)

	// 1. Empresa B chega primeiro, mas com TimestampLamport = 50
	reqEmpresaB := utils.Requisicao2PCPrepare{
		TransacaoID:      "TX-B-1",
		ReservaID:        "RES-B-1",
		PassageiroID:     "pb@email.com",
		EmpresaOrigemID:  "empresaB",
		CoordenadorURL:   "http://empresaB:9082",
		TimestampLamport: 50,
		TrechosLocais: []utils.ReferenciaTrechoLocal{{
			CaronaID: "CAR-C-DISPUTA",
			Origem:   "Vitória da Conquista",
			Destino:  "Montes Claros",
		}},
	}
	if ok, msg := utils.ExecutarPrepareLocal(reqEmpresaB); !ok {
		t.Fatalf("Prepare da Empresa B deveria ter passado: %s", msg)
	}

	// 2. Empresa A chega logo depois com MESMO TimestampLamport = 50!
	// Pelo critério de desempate determinístico ("empresaA" < "empresaB"), a Empresa A deve vencer!
	reqEmpresaAEmpate := utils.Requisicao2PCPrepare{
		TransacaoID:      "TX-A-1",
		ReservaID:        "RES-A-1",
		PassageiroID:     "pa@email.com",
		EmpresaOrigemID:  "empresaA",
		CoordenadorURL:   "http://empresaA:9081",
		TimestampLamport: 50, // Empate no relógio de Lamport -> Desempate por EmpresaOrigemID!
		TrechosLocais: []utils.ReferenciaTrechoLocal{{
			CaronaID: "CAR-C-DISPUTA",
			Origem:   "Vitória da Conquista",
			Destino:  "Montes Claros",
		}},
	}
	if ok, msg := utils.ExecutarPrepareLocal(reqEmpresaAEmpate); !ok {
		t.Fatalf("No empate de Lamport=50, a empresaA deveria vencer a empresaB por ordem alfabética: %s", msg)
	}

	// Confirma que a Empresa B perdeu o assento e a Empresa A conseguiu dar COMMIT
	if okB, _ := utils.ExecutarCommitLocal("TX-B-1", "RES-B-1"); okB {
		t.Fatalf("Empresa B não poderia conseguir dar COMMIT após sofrer preempção!")
	}
	if okA, _ := utils.ExecutarCommitLocal("TX-A-1", "RES-A-1"); !okA {
		t.Fatalf("Empresa A vencedora deveria conseguir dar COMMIT!")
	}

	t.Log("[OK TESTE 6]: Precedência de Lamport e desempate determinístico (empresaA < empresaB em tempos iguais) validados!")
}

// ============================================================================
// TESTE 7: Consulta de Passageiros e Cancelamento de Carona Entre Empresas 100% via REST
// ============================================================================
func Test7_ConsultaPassageirosECancelamentoEntreEmpresasViaREST(t *testing.T) {
	cleanup := configurarAmbienteTesteIsolado(t, "empresaA")
	defer cleanup()

	srvA := httptest.NewServer(utils.ConfigurarRotasConsorcio())
	defer srvA.Close()
	utils.ServidorAtualURL = srvA.URL

	// Publica carona na Empresa A (motorista motA@email.com)
	caronaA := utils.Carona{
		ID:          "CAR-A-REST",
		MotoristaID: "motA@email.com",
		AssentosTot: 2,
		Ativa:       true,
		Trechos: []utils.Trecho{{
			Origem:         "Salvador",
			Destino:        "Feira de Santana",
			DataPartida:    utils.NovaData(20, 11, 2026),
			HorarioPartida: time.Now(),
			HorarioChegada: time.Now().Add(time.Hour),
			Preco:          25.0,
			AssentosLivre:  1, // 1 assento já ocupado pelo passageiro da Empresa B
		}},
	}
	_ = utils.SalvarCarona(caronaA)

	// Simula o servidor da Empresa B (onde está o passageiro "maria_empB@email.com" que comprou CAR-A-REST + CAR-C-OUTRA)
	var muB sync.Mutex
	reservaAtivaNaEmpresaB := true
	 liberouOutroTrechoNaEmpresaC := false

	srvC := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/reservas/liberar" {
			muB.Lock()
			liberouOutroTrechoNaEmpresaC = true
			muB.Unlock()
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]bool{"sucesso": true})
	}))
	defer srvC.Close()

	muxB := http.NewServeMux()
	// Quando a Empresa A perguntar via GET /api/caronas/passageiros quem está na CAR-A-REST:
	muxB.HandleFunc("/api/caronas/passageiros", func(w http.ResponseWriter, r *http.Request) {
		muB.Lock()
		defer muB.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if reservaAtivaNaEmpresaB && r.URL.Query().Get("carona_id") == "CAR-A-REST" {
			_ = json.NewEncoder(w).Encode([]string{"maria_empB@email.com"})
			return
		}
		_ = json.NewEncoder(w).Encode([]string{})
	})
	// Quando a Empresa A avisar via POST /api/caronas/cancelada que o motorista cancelou a CAR-A-REST:
	muxB.HandleFunc("/api/caronas/cancelada", func(w http.ResponseWriter, r *http.Request) {
		muB.Lock()
		reservaAtivaNaEmpresaB = false
		muB.Unlock()
		// A Empresa B apaga a reserva da Maria e libera o trecho 2 na Empresa C via REST
		utils.EnviarLiberacaoRemota(srvC.URL, "RES-MARIA-1", []utils.ArestaTrecho{{
			CaronaID: "CAR-C-OUTRA", Origem: "Feira de Santana", Destino: "Alagoinhas",
		}})
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]bool{"sucesso": true})
	})
	srvB := httptest.NewServer(muxB)
	defer srvB.Close()

	utils.ConfigurarPeers(srvB.URL)

	// 1. Motorista da Empresa A consulta passageiros da CAR-A-REST (deve achar "maria_empB@email.com" via REST!)
	passageiros, err := utils.ConsultarPassageirosCarona("CAR-A-REST", "motA@email.com")
	if err != nil || len(passageiros) != 1 || passageiros[0] != "maria_empB@email.com" {
		t.Fatalf("Esperava encontrar a passageira remota maria_empB@email.com via REST, mas obteve: %v (err=%v)", passageiros, err)
	}

	// 2. Motorista da Empresa A cancela a CAR-A-REST (primeiro pede confirmação pois há passageiro remoto)
	precisaConfirmar, _, err := utils.CancelarCaronaMotorista("CAR-A-REST", "motA@email.com", false)
	if err != nil || !precisaConfirmar {
		t.Fatalf("Deveria exigir confirmação ao motorista pois há passageira remota na carona!")
	}

	// 3. Motorista confirma o cancelamento: deve notificar a Empresa B via POST /api/caronas/cancelada,
	// que apaga a reserva e libera o outro trecho na Empresa C via POST /api/reservas/liberar!
	precisaConfirmar, _, err = utils.CancelarCaronaMotorista("CAR-A-REST", "motA@email.com", true)
	if err != nil || precisaConfirmar {
		t.Fatalf("Falha ao confirmar cancelamento da carona: %v", err)
	}

	muB.Lock()
	cancelouNaB := !reservaAtivaNaEmpresaB
	liberouNaC := liberouOutroTrechoNaEmpresaC
	muB.Unlock()

	if !cancelouNaB || !liberouNaC {
		t.Fatalf("O cancelamento da carona via REST não propagou para os outros servidores! cancelouNaB=%v, liberouNaC=%v",
			cancelouNaB, liberouNaC)
	}

	t.Log("[OK TESTE 7]: Consulta de passageiros e cancelamento de carona em cascata entre empresas 100% via REST validados!")
}
