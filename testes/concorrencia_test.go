package testes

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"vaiJunto/utils"
)

var enderecoServidor string

func TestMain(m *testing.M) {
	flag.StringVar(&enderecoServidor, "server", "localhost:8080", "Endereço do servidor TCP (ex: 192.168.1.15:8080)")
	flag.Parse()

	exitCode := m.Run()
	os.Exit(exitCode)
}

// Função auxiliar genérica para enviar requisições TCP e ler respostas
func enviarRequisicao(conn net.Conn, acao utils.TipoAcao, usuario string, payload interface{}) (utils.MensagemResposta, error) {
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

	buf := make([]byte, 8192)
	n, err := conn.Read(buf)
	if err != nil {
		return utils.MensagemResposta{}, err
	}

	var resp utils.MensagemResposta
	err = json.Unmarshal(buf[:n], &resp)
	return resp, err
}

// 1. Teste Unitário: Normalização de Texto
func TestNormalizacaoDeTexto(t *testing.T) {
	entrada := "   Feira DE Santana   "
	esperado := "feira de santana"

	resultado := utils.NormalizarTexto(entrada)
	if resultado != esperado {
		t.Fatalf("Erro na normalização: esperava '%s', obteve '%s'", esperado, resultado)
	}
	t.Log("[UNITÁRIO]: Normalização de texto validada!")
}

// 2. Teste Unitário: Consolidação de Trechos
func TestConsolidacaoDeTrechos(t *testing.T) {
	trechosOriginais := []utils.ArestaTrecho{
		{CaronaID: "CAR-1", Origem: "Feira de Santana", Destino: "Santo Amaro", Preco: 15.0, AssentosLivre: 3},
		{CaronaID: "CAR-1", Origem: "Santo Amaro", Destino: "Salvador", Preco: 20.0, AssentosLivre: 2},
	}

	consolidados := utils.ConsolidarTrechos(trechosOriginais)

	if len(consolidados) != 1 {
		t.Fatalf("Deveria ter consolidado em 1 único trecho, mas gerou %d", len(consolidados))
	}

	if consolidados[0].Preco != 35.0 || consolidados[0].AssentosLivre != 2 {
		t.Fatalf("A soma de preço ou o menor limite de assentos falhou na consolidação")
	}
	t.Log("[UNITÁRIO]: Consolidação de trechos contínuos validada!")
}

// 3. Teste Unitário: Formatação de Data
func TestEstruturaData(t *testing.T) {
	data := utils.NovaData(15, 10, 2026)
	esperado := "15/10/2026"

	if data.String() != esperado {
		t.Fatalf("Erro ao formatar data: esperava %s, obteve %s", esperado, data.String())
	}
	t.Log("[UNITÁRIO]: Formatação de data validada!")
}

// 4. Teste de Integração: Cadastro e Autenticação Concorrente
func TestConcorrenciaAutenticacaoECadastro(t *testing.T) {
	const totalUsuarios = 15
	var wg sync.WaitGroup
	var mu sync.Mutex

	sucessos := 0
	falhas := 0

	wg.Add(totalUsuarios)
	for i := 1; i <= totalUsuarios; i++ {
		email := fmt.Sprintf("user_stress_%d@email.com", i)
		go func(e string) {
			defer wg.Done()
			conn, err := net.Dial("tcp", enderecoServidor)
			if err != nil {
				mu.Lock()
				falhas++
				mu.Unlock()
				return
			}
			defer conn.Close()

			payloadAuth := map[string]interface{}{
				"email": e,
				"senha": "senha_segura_123",
				"tipo":  utils.TipoPassageiro,
			}

			resp, err := enviarRequisicao(conn, utils.AcaoAutenticar, e, payloadAuth)
			mu.Lock()
			if err == nil && resp.Sucesso {
				sucessos++
			} else {
				falhas++
			}
			mu.Unlock()
		}(email)
	}

	wg.Wait()
	t.Logf("[INTEGRAÇÃO]: %d cadastrados com sucesso | %d falhas", sucessos, falhas)
	if sucessos != totalUsuarios {
		t.Fatalf("Erro no cadastro concorrente: esperava %d sucessos, obteve %d", totalUsuarios, sucessos)
	}
}

// 5. Teste de Integração: Busca Concorrente no Grafo (BFS) com Data Corrigida
func TestConcorrenciaBuscaItinerarios(t *testing.T) {
	conn, err := net.Dial("tcp", enderecoServidor)
	if err != nil {
		t.Fatalf("Servidor offline: %v", err)
	}
	defer conn.Close()

	caronaID := fmt.Sprintf("CAR-BUSCA-%d", time.Now().UnixNano())
	c := utils.Carona{
		ID:          caronaID,
		MotoristaID: "m_busca@email.com",
		AssentosTot: 4,
		Ativa:       true,
		Trechos: []utils.Trecho{{
			Origem:         "Feira de Santana",
			Destino:        "Salvador",
			DataPartida:    utils.NovaData(15, 10, 2026),
			HorarioPartida: time.Now(),
			HorarioChegada: time.Now().Add(time.Hour),
			Preco:          25.0,
			AssentosLivre:  4,
		}},
	}
	enviarRequisicao(conn, utils.AcaoPublicarCarona, "m_busca@email.com", c)

	var wg sync.WaitGroup
	var mu sync.Mutex
	buscasSucesso := 0

	filtroBusca := map[string]interface{}{
		"origem":  "  FEIRA de santana ",
		"destino": "salvador  ",
		"data":    utils.NovaData(15, 10, 2026),
	}

	wg.Add(20)
	for i := 0; i < 20; i++ {
		go func() {
			defer wg.Done()
			cli, err := net.Dial("tcp", enderecoServidor)
			if err != nil {
				return
			}
			defer cli.Close()

			resp, err := enviarRequisicao(cli, utils.AcaoBuscarItinerario, "passageiro_busca@email.com", filtroBusca)
			if err == nil && resp.Sucesso {
				mu.Lock()
				buscasSucesso++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	t.Logf("[INTEGRAÇÃO]: %d buscas concorrentes no Grafo concluídas", buscasSucesso)
	if buscasSucesso != 20 {
		t.Fatalf("Falha na consulta concorrente ao Grafo: esperava 20, obteve %d", buscasSucesso)
	}
}

// 6. Teste de Integração: Disparo Concorrente em Itinerário Composto
func TestConcorrenciaItinerarioComposto(t *testing.T) {
	conn, err := net.Dial("tcp", enderecoServidor)
	if err != nil {
		t.Fatalf("Servidor offline: %v", err)
	}
	defer conn.Close()

	carona1ID := fmt.Sprintf("CAR-COMP1-%d", time.Now().UnixNano())
	c1 := utils.Carona{
		ID:          carona1ID,
		MotoristaID: "m1@email.com",
		AssentosTot: 1,
		Ativa:       true,
		Trechos: []utils.Trecho{{
			Origem: "Feira de Santana", Destino: "Santo Amaro",
			DataPartida: utils.NovaData(15, 10, 2026), HorarioPartida: time.Now(), HorarioChegada: time.Now().Add(time.Hour),
			Preco: 15.0, AssentosLivre: 1,
		}},
	}
	enviarRequisicao(conn, utils.AcaoPublicarCarona, "m1@email.com", c1)

	carona2ID := fmt.Sprintf("CAR-COMP2-%d", time.Now().UnixNano())
	c2 := utils.Carona{
		ID:          carona2ID,
		MotoristaID: "m2@email.com",
		AssentosTot: 1,
		Ativa:       true,
		Trechos: []utils.Trecho{{
			Origem: "Santo Amaro", Destino: "Salvador",
			DataPartida: utils.NovaData(15, 10, 2026), HorarioPartida: time.Now().Add(2 * time.Hour), HorarioChegada: time.Now().Add(3 * time.Hour),
			Preco: 20.0, AssentosLivre: 1,
		}},
	}
	enviarRequisicao(conn, utils.AcaoPublicarCarona, "m2@email.com", c2)

	itinerarioComposto := utils.Itinerario{
		PrecoTotal: 35.0,
		Trechos: []utils.ArestaTrecho{
			{CaronaID: carona1ID, Origem: "Feira de Santana", Destino: "Santo Amaro", Preco: 15.0},
			{CaronaID: carona2ID, Origem: "Santo Amaro", Destino: "Salvador", Preco: 20.0},
		},
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	reservasAprovadas := 0

	wg.Add(10)
	for i := 1; i <= 10; i++ {
		email := fmt.Sprintf("p_comp_%d@email.com", i)
		go func(e string) {
			defer wg.Done()
			c, err := net.Dial("tcp", enderecoServidor)
			if err != nil {
				return
			}
			defer c.Close()

			resp, err := enviarRequisicao(c, utils.AcaoReservarTrecho, e, itinerarioComposto)
			if err == nil && resp.Sucesso {
				mu.Lock()
				reservasAprovadas++
				mu.Unlock()
			}
		}(email)
	}

	wg.Wait()
	t.Logf("[INTEGRAÇÃO]: %d aprovado(s) para itinerário atômico", reservasAprovadas)
	if reservasAprovadas != 1 {
		t.Fatalf("Erro na atomicidade do itinerário: esperava 1, obteve %d", reservasAprovadas)
	}
}

// 7. Teste de Integração: Gestão de Reservas (Listar e Cancelar)
func TestConcorrenciaListarECancelarReserva(t *testing.T) {
	conn, err := net.Dial("tcp", enderecoServidor)
	if err != nil {
		t.Fatalf("Servidor offline: %v", err)
	}
	defer conn.Close()

	caronaID := fmt.Sprintf("CAR-GEST-%d", time.Now().UnixNano())
	c := utils.Carona{
		ID:          caronaID,
		MotoristaID: "m_gestao@email.com",
		AssentosTot: 2,
		Ativa:       true,
		Trechos: []utils.Trecho{{
			Origem: "Feira de Santana", Destino: "Valença",
			DataPartida: utils.NovaData(15, 10, 2026), HorarioPartida: time.Now(), HorarioChegada: time.Now().Add(time.Hour),
			Preco: 40.0, AssentosLivre: 2,
		}},
	}
	enviarRequisicao(conn, utils.AcaoPublicarCarona, "m_gestao@email.com", c)

	itin := utils.Itinerario{
		PrecoTotal: 40.0,
		Trechos:    []utils.ArestaTrecho{{CaronaID: caronaID, Origem: "Feira de Santana", Destino: "Valença"}},
	}

	respRes, _ := enviarRequisicao(conn, utils.AcaoReservarTrecho, "passageiro_gestao@email.com", itin)
	var resObj utils.Reserva
	json.Unmarshal(respRes.Payload, &resObj)

	respLista, err := enviarRequisicao(conn, utils.AcaoListarReservas, "passageiro_gestao@email.com", nil)
	if err != nil || !respLista.Sucesso {
		t.Fatalf("Falha ao listar reservas do passageiro")
	}

	payloadCanc := map[string]string{"reserva_id": resObj.ID}
	respCanc, err := enviarRequisicao(conn, utils.AcaoCancelarReserva, "passageiro_gestao@email.com", payloadCanc)
	if err != nil || !respCanc.Sucesso {
		t.Fatalf("Falha ao cancelar reserva: %s", respCanc.Mensagem)
	}

	t.Log("[INTEGRAÇÃO]: Listagem e cancelamento de reservas validados")
}

// 8. Teste de Integração: Consultas Exclusivas do Motorista
func TestConcorrenciaConsultasMotorista(t *testing.T) {
	conn, err := net.Dial("tcp", enderecoServidor)
	if err != nil {
		t.Fatalf("Servidor offline: %v", err)
	}
	defer conn.Close()

	emailMotorista := "motorista_consultas@email.com"
	caronaID := fmt.Sprintf("CAR-CONS-%d", time.Now().UnixNano())

	c := utils.Carona{
		ID:          caronaID,
		MotoristaID: emailMotorista,
		AssentosTot: 3,
		Ativa:       true,
		Trechos: []utils.Trecho{{
			Origem: "Feira de Santana", Destino: "Ilhéus",
			DataPartida: utils.NovaData(15, 10, 2026), HorarioPartida: time.Now(), HorarioChegada: time.Now().Add(time.Hour),
			Preco: 80.0, AssentosLivre: 3,
		}},
	}
	enviarRequisicao(conn, utils.AcaoPublicarCarona, emailMotorista, c)

	itin := utils.Itinerario{
		PrecoTotal: 80.0,
		Trechos:    []utils.ArestaTrecho{{CaronaID: caronaID, Origem: "Feira de Santana", Destino: "Ilhéus"}},
	}
	enviarRequisicao(conn, utils.AcaoReservarTrecho, "p_ilheus@email.com", itin)

	respConsult, _ := enviarRequisicao(conn, utils.AcaoConsultarCaronas, emailMotorista, nil)
	if !respConsult.Sucesso {
		t.Fatalf("Motorista falhou ao consultar suas caronas")
	}

	respPass, _ := enviarRequisicao(conn, utils.AcaoConsultarPassageirosCarona, emailMotorista, map[string]string{"carona_id": caronaID})
	if !respPass.Sucesso {
		t.Fatalf("Motorista falhou ao consultar passageiros da carona")
	}

	t.Log("[INTEGRAÇÃO]: Consultas exclusivas do motorista validadas!")
}

// 9. Teste de Integração: Cancelamento de Carona pelo Motorista
func TestCancelarCaronaMotorista(t *testing.T) {
	conn, err := net.Dial("tcp", enderecoServidor)
	if err != nil {
		t.Fatalf("Servidor offline: %v", err)
	}
	defer conn.Close()

	emailMotorista := "motorista_cancela@email.com"
	caronaID := fmt.Sprintf("CAR-CANC-MOT-%d", time.Now().UnixNano())

	c := utils.Carona{
		ID:          caronaID,
		MotoristaID: emailMotorista,
		AssentosTot: 2,
		Ativa:       true,
		Trechos: []utils.Trecho{{
			Origem: "Feira de Santana", Destino: "Itabuna",
			DataPartida: utils.NovaData(15, 10, 2026), HorarioPartida: time.Now(), HorarioChegada: time.Now().Add(time.Hour),
			Preco: 60.0, AssentosLivre: 2,
		}},
	}
	enviarRequisicao(conn, utils.AcaoPublicarCarona, emailMotorista, c)

	payloadCanc := map[string]interface{}{"carona_id": caronaID, "confirmar": true}
	respCancMot, _ := enviarRequisicao(conn, utils.AcaoCancelarCarona, emailMotorista, payloadCanc)
	if !respCancMot.Sucesso {
		t.Fatalf("Falha ao cancelar carona pelo motorista: %s", respCancMot.Mensagem)
	}

	t.Log("[INTEGRAÇÃO]: Cancelamento de carona pelo motorista validado!")
}

// 10. Teste de Integração: Bloqueio de Logins Simultâneos com o Mesmo E-mail
func TestBloqueioLoginSimultaneo(t *testing.T) {
	// 1. Abre a primeira conexão
	conn1, err := net.Dial("tcp", enderecoServidor)
	if err != nil {
		t.Fatalf("Servidor offline: %v", err)
	}
	defer conn1.Close()

	emailTeste := "usuario_duplicado@email.com"
	payloadAuth := map[string]interface{}{
		"email": emailTeste,
		"senha": "senha123",
		"tipo":  utils.TipoPassageiro,
	}

	// 2. Realiza o primeiro login com sucesso
	resp1, err := enviarRequisicao(conn1, utils.AcaoAutenticar, emailTeste, payloadAuth)
	if err != nil || !resp1.Sucesso {
		t.Fatalf("Primeiro login deveria ter sido bem-sucedido: %s", resp1.Mensagem)
	}

	// 3. Abre uma segunda conexão para tentar logar com o MESMO e-mail simultaneamente
	conn2, err := net.Dial("tcp", enderecoServidor)
	if err != nil {
		t.Fatalf("Erro ao abrir segunda conexão: %v", err)
	}
	defer conn2.Close()

	resp2, err := enviarRequisicao(conn2, utils.AcaoAutenticar, emailTeste, payloadAuth)
	if err != nil {
		t.Fatalf("Erro de comunicação no segundo login: %v", err)
	}

	// 4. O servidor deve rejeitar a segunda tentativa de login
	if resp2.Sucesso {
		t.Fatalf("Falha de lógica: O servidor permitiu login simultâneo com o mesmo e-mail!")
	}

	t.Log("[INTEGRAÇÃO]: Bloqueio de login simultâneo validado com sucesso!")
}

// 11. Teste de Integração: Queda Abrupta de Cliente (EOF)
func TestQuedaAbruptaDeCliente(t *testing.T) {
	for i := 0; i < 5; i++ {
		conn, err := net.Dial("tcp", enderecoServidor)
		if err == nil {
			conn.Close()
		}
	}

	connOk, err := net.Dial("tcp", enderecoServidor)
	if err != nil {
		t.Fatalf("Servidor caiu após quedas abruptas de conexão!")
	}
	defer connOk.Close()

	resp, err := enviarRequisicao(connOk, utils.AcaoConsultarCaronas, "teste_resiliencia@email.com", nil)
	if err != nil || !resp.Sucesso {
		t.Fatalf("Servidor deixou de responder requisições após falhas nos sockets")
	}

	t.Log("O servidor tratou os erros de socket (EOF) e permanece online!")
}

// 12. Teste de Integração: Disputa Concorrente pelo Mesmo Assento (Atomicidade / Tudo ou Nada)
func TestConcorrenciaDisputaMesmoAssento(t *testing.T) {
	conn, err := net.Dial("tcp", enderecoServidor)
	if err != nil {
		t.Fatalf("Servidor offline: %v", err)
	}
	defer conn.Close()

	caronaID := fmt.Sprintf("CAR-DISPUTA-%d", time.Now().UnixNano())
	c := utils.Carona{
		ID:          caronaID,
		MotoristaID: "m_disputa@email.com",
		AssentosTot: 1, // Apenas 1 assento disponível na carona inteira!
		Ativa:       true,
		Trechos: []utils.Trecho{{
			Origem:         "Feira de Santana",
			Destino:        "Salvador",
			DataPartida:    utils.NovaData(15, 10, 2026),
			HorarioPartida: time.Now(),
			HorarioChegada: time.Now().Add(time.Hour),
			Preco:          30.0,
			AssentosLivre:  1, // Apenas 1 vaga real
		}},
	}
	enviarRequisicao(conn, utils.AcaoPublicarCarona, "m_disputa@email.com", c)

	itinerarioDesejado := utils.Itinerario{
		PrecoTotal: 30.0,
		Trechos: []utils.ArestaTrecho{{
			CaronaID: caronaID,
			Origem:   "Feira de Santana",
			Destino:  "Salvador",
			Preco:    30.0,
		}},
	}

	const totalClientesDisputando = 15
	var wg sync.WaitGroup
	var mu sync.Mutex
	sucessosReserva := 0
	falhasReserva := 0

	wg.Add(totalClientesDisputando)
	for i := 1; i <= totalClientesDisputando; i++ {
		emailPassageiro := fmt.Sprintf("passageiro_lento_%d@email.com", i)
		go func(email string) {
			defer wg.Done()
			cli, err := net.Dial("tcp", enderecoServidor)
			if err != nil {
				return
			}
			defer cli.Close()

			resp, err := enviarRequisicao(cli, utils.AcaoReservarTrecho, email, itinerarioDesejado)
			mu.Lock()
			defer mu.Unlock()
			if err == nil && resp.Sucesso {
				sucessosReserva++
			} else {
				falhasReserva++
			}
		}(emailPassageiro)
	}

	wg.Wait()

	t.Logf("[INTEGRAÇÃO]: Resultado da disputa pelo único assento -> Sucessos: %d | Falhas rejeitadas corretamente: %d", sucessosReserva, falhasReserva)

	// Validação estrita exigida pelo edital: Apenas 1 pode vencer, os outros devem falhar por falta de vaga
	if sucessosReserva != 1 {
		t.Fatalf("FALHA DE ATOMICIDADE: O sistema permitiu %d reservas para 1 único assento!", sucessosReserva)
	}
	if falhasReserva != totalClientesDisputando-1 {
		t.Fatalf("Inconsistência nas rejeições: esperava %d falhas, obteve %d", totalClientesDisputando-1, falhasReserva)
	}
	t.Log("[INTEGRAÇÃO]: Teste de concorrência no mesmo assento validado com sucesso! Nenhum assento vendido em duplicidade.")
}

// 13. Teste de Integração: Acesso Simultâneo a Corridas Diferentes (Concorrência sem Bloqueio Indevido)
func TestConcorrenciaCorridasDiferentes(t *testing.T) {
	conn, err := net.Dial("tcp", enderecoServidor)
	if err != nil {
		t.Fatalf("Servidor offline: %v", err)
	}
	defer conn.Close()

	// Publica 5 caronas totalmente independentes, cada uma com 5 assentos livres
	const totalCaronas = 5
	var caronaIDs []string

	for i := 1; i <= totalCaronas; i++ {
		cID := fmt.Sprintf("CAR-INDIV-%d-%d", i, time.Now().UnixNano())
		caronaIDs = append(caronaIDs, cID)

		c := utils.Carona{
			ID:          cID,
			MotoristaID: fmt.Sprintf("motorista_%d@email.com", i),
			AssentosTot: 5,
			Ativa:       true,
			Trechos: []utils.Trecho{{
				Origem:         "Cidade A",
				Destino:        "Cidade B",
				DataPartida:    utils.NovaData(20, 10, 2026),
				HorarioPartida: time.Now(),
				HorarioChegada: time.Now().Add(time.Hour),
				Preco:          50.0,
				AssentosLivre:  5,
			}},
		}
		enviarRequisicao(conn, utils.AcaoPublicarCarona, c.MotoristaID, c)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	reservasIndependentesSucesso := 0

	// 5 passageiros diferentes reservando caronas DIFERENTES ao mesmo tempo em paralelo
	wg.Add(totalCaronas)
	for i := 0; i < totalCaronas; i++ {
		idx := i
		go func() {
			defer wg.Done()
			cli, err := net.Dial("tcp", enderecoServidor)
			if err != nil {
				return
			}
			defer cli.Close()

			itin := utils.Itinerario{
				PrecoTotal: 50.0,
				Trechos: []utils.ArestaTrecho{{
					CaronaID: caronaIDs[idx],
					Origem:   "Cidade A",
					Destino:  "Cidade_B",
					Preco:    50.0,
				}},
			}
			// Ajustando o destino corretamente para "Cidade B"
			itin.Trechos[0].Destino = "Cidade B"

			emailPassageiro := fmt.Sprintf("passageiro_independente_%d@email.com", idx)
			resp, err := enviarRequisicao(cli, utils.AcaoReservarTrecho, emailPassageiro, itin)

			mu.Lock()
			if err == nil && resp.Sucesso {
				reservasIndependentesSucesso++
			}
			mu.Unlock()
		}()
	}

	wg.Wait()

	t.Logf("[INTEGRAÇÃO]: %d reservas em caronas distintas processadas concorrentemente com sucesso", reservasIndependentesSucesso)

	if reservasIndependentesSucesso != totalCaronas {
		t.Fatalf("Erro de concorrência cruzada: esperava que todas as %d reservas em rotas distintas passassem, mas passaram apenas %d", totalCaronas, reservasIndependentesSucesso)
	}
	t.Log("[INTEGRAÇÃO]: Comprovado que acessos a corridas diferentes ocorrem concorrentemente sem interferência mutua!")
}

// 14. Teste de Integração: Envio de Mensagem Malformada (Validação e Robustez)
func TestEnvioMensagemMalformada(t *testing.T) {
	conn, err := net.Dial("tcp", enderecoServidor)
	if err != nil {
		t.Fatalf("Servidor offline: %v", err)
	}
	defer conn.Close()

	// Envia bytes que não formam um JSON estruturado corretamente
	dadosMalformados := []byte("{ \"acao\": \"PUBLICAR_CARONA\", \"payload\": dados_quebrados_sem_fechar_aspas... }}} \n")
	if _, err := conn.Write(dadosMalformados); err != nil {
		t.Fatalf("Erro ao enviar dados malformados: %v", err)
	}

	// Pausa breve para o servidor processar e descartar a mensagem no log/parser
	time.Sleep(100 * time.Millisecond)

	// O teste de ouro da resiliência: O servidor NÃO PODE ter caído.
	// Abrimos uma nova conexão válida logo em seguida para provar que ele continua operando.
	connValida, err := net.Dial("tcp", enderecoServidor)
	if err != nil {
		t.Fatalf("FALHA DE RESILIÊNCIA: O servidor caiu após receber uma mensagem malformada!")
	}
	defer connValida.Close()

	_, err = enviarRequisicao(connValida, utils.AcaoConsultarCaronas, "teste_robustez@email.com", nil)
	if err != nil {
		t.Fatalf("O servidor parou de responder requisições após o erro de parsing.")
	}

	t.Log("[INTEGRAÇÃO]: Robustez do parser contra mensagens malformadas validada com sucesso! Servidor descartou o erro e continuou ativo.")
}

// 15. Teste de Desempenho: Medição de Tempo de Resposta sob Carga
func TestDesempenhoTempoResposta(t *testing.T) {
	conn, err := net.Dial("tcp", enderecoServidor)
	if err != nil {
		t.Fatalf("Servidor offline: %v", err)
	}
	defer conn.Close()

	// Publica uma carona com vários assentos para suportar múltiplos testes de busca/reserva
	caronaID := fmt.Sprintf("CAR-PERF-%d", time.Now().UnixNano())
	c := utils.Carona{
		ID:          caronaID,
		MotoristaID: "m_perf@email.com",
		AssentosTot: 50,
		Ativa:       true,
		Trechos: []utils.Trecho{{
			Origem:         "Feira de Santana",
			Destino:        "Salvador",
			DataPartida:    utils.NovaData(15, 10, 2026),
			HorarioPartida: time.Now(),
			HorarioChegada: time.Now().Add(time.Hour),
			Preco:          30.0,
			AssentosLivre:  50,
		}},
	}
	enviarRequisicao(conn, utils.AcaoPublicarCarona, "m_perf@email.com", c)

	const totalRequisicoes = 50
	var wg sync.WaitGroup
	var mu sync.Mutex

	// Fila para armazenar a latência de cada requisição individual
	latencias := make([]time.Duration, 0, totalRequisicoes)

	wg.Add(totalRequisicoes)
	for i := 1; i <= totalRequisicoes; i++ {
		email := fmt.Sprintf("p_perf_%d@email.com", i)
		go func(e string) {
			defer wg.Done()
			cli, err := net.Dial("tcp", enderecoServidor)
			if err != nil {
				return
			}
			defer cli.Close()

			filtroBusca := map[string]interface{}{
				"origem":  "Feira de Santana",
				"destino": "Salvador",
				"data":    utils.NovaData(15, 10, 2026),
			}

			// CAPTURA O TEMPO ANTES DA REQUISIÇÃO
			inicio := time.Now()
			_, err = enviarRequisicao(cli, utils.AcaoBuscarItinerario, e, filtroBusca)
			// MEDIÇÃO DO TEMPO DE RESPOSTA (ROUND-TRIP)
			duracao := time.Since(inicio)

			if err == nil {
				mu.Lock()
				latencias = append(latencias, duracao)
				mu.Unlock()
			}
		}(email)
	}

	wg.Wait()

	// Calcula estatísticas de desempenho (Média, Mínimo e Máximo)
	if len(latencias) == 0 {
		t.Fatalf("Nenhuma requisição de desempenho foi concluída com sucesso.")
	}

	var soma time.Duration
	min := latencias[0]
	max := latencias[0]

	for _, lat := range latencias {
		soma += lat
		if lat < min {
			min = lat
		}
		if lat > max {
			max = lat
		}
	}
	media := soma / time.Duration(len(latencias))

	// Exibe os resultados no console (ótimos para prints do relatório / apresentação)
	t.Logf("==================================================")
	t.Logf("[DESEMPENHO SOB CARGA] Total de Requisições: %d", len(latencias))
	t.Logf("⏱️ Tempo de Resposta Médio:  %v", media)
	t.Logf("🚀 Menor Tempo (Mais rápido): %v", min)
	t.Logf("⏳ Maior Tempo (Mais lento):  %v", max)
	t.Logf("==================================================")
}

// 16. Teste do Consórcio (Problema 2): Sob Carga e com Injeção de Falhas (Two-Phase Commit, Lamport e Recovery)
// Verifica que:
//   (a) Nenhum assento é confirmado para mais de um passageiro sob disputa simultânea;
//   (b) Nenhum itinerário é confirmado pela metade quando o servidor de uma empresa falha;
//   (c) Nenhum assento permanece bloqueado após a queda e recuperação de um servidor participante/coordenador.
func TestConsorcio2PCSobCargaEInjecaoDeFalhas(t *testing.T) {
	pastaTemp, err := os.MkdirTemp("", "teste_consorcio_2pc_*")
	if err != nil {
		t.Fatalf("Erro ao criar diretório temporário: %v", err)
	}
	defer os.RemoveAll(pastaTemp)

	// Salva a configuração original e restaura ao final do teste
	origCaronas := utils.ArquivoCaronas
	origReservas := utils.ArquivoReservas
	origTransacoes := utils.ArquivoTransacoes
	origEmp := utils.EmpresaAtualID
	origURL := utils.ServidorAtualURL
	defer func() {
		utils.ArquivoCaronas = origCaronas
		utils.ArquivoReservas = origReservas
		utils.ArquivoTransacoes = origTransacoes
		utils.EmpresaAtualID = origEmp
		utils.ServidorAtualURL = origURL
	}()

	utils.ArquivoCaronas = pastaTemp + "/caronas.json"
	utils.ArquivoReservas = pastaTemp + "/reservas.json"
	utils.ArquivoTransacoes = pastaTemp + "/transacoesgerais.json"
	utils.EmpresaAtualID = "empresaA"

	// Sobe o servidor HTTP REST da Empresa A (local)
	srvA := httptest.NewServer(utils.ConfigurarRotasConsorcio())
	defer srvA.Close()
	utils.ServidorAtualURL = srvA.URL

	// Simula o servidor HTTP REST da Empresa B (com controle de vagas e injeção de falha)
	var muB sync.Mutex
	assentosB := 1
	empresaBAtiva := true
	var transacoesB = make(map[string]utils.EstadoTransacao)

	muxB := http.NewServeMux()
	muxB.HandleFunc("/api/2pc/prepare", func(w http.ResponseWriter, r *http.Request) {
		muB.Lock()
		defer muB.Unlock()
		if !empresaBAtiva {
			http.Error(w, "servidor Empresa B fora do ar", http.StatusServiceUnavailable)
			return
		}
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

	// Publica carona local na Empresa A com apenas 1 assento livre (Salvador -> Feira)
	caronaA := utils.Carona{
		ID:          "CAR-EMP-A-1",
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
	if err := utils.SalvarCarona(caronaA); err != nil {
		t.Fatalf("Falha ao salvar carona na Empresa A: %v", err)
	}

	itinInterEmpresas := utils.Itinerario{
		PrecoTotal: 90.0,
		Trechos: []utils.ArestaTrecho{
			{
				CaronaID:    "CAR-EMP-A-1",
				EmpresaID:   "empresaA",
				ServidorURL: srvA.URL,
				Origem:      "Salvador",
				Destino:     "Feira de Santana",
				Preco:       25.0,
			},
			{
				CaronaID:    "CAR-EMP-B-1",
				EmpresaID:   "empresaB",
				ServidorURL: srvB.URL,
				Origem:      "Feira de Santana",
				Destino:     "Vitória da Conquista",
				Preco:       65.0,
			},
		},
	}

	// PARTE A: 20 passageiros sob carga disputando simultaneamente o único assento inter-empresas
	const concorrentes = 20
	var wg sync.WaitGroup
	var muCont sync.Mutex
	confirmados := 0

	wg.Add(concorrentes)
	for i := 0; i < concorrentes; i++ {
		passageiro := fmt.Sprintf("p_consorcio_%d@email.com", i)
		go func(p string) {
			defer wg.Done()
			_, err := utils.CoordenarReserva2PC(p, itinInterEmpresas)
			if err == nil {
				muCont.Lock()
				confirmados++
				muCont.Unlock()
			}
		}(passageiro)
	}
	wg.Wait()

	if confirmados != 1 {
		t.Fatalf("[FALHA 2PC SOB CARGA]: Esperava exatamente 1 reserva confirmada para 1 assento, mas obteve %d", confirmados)
	}
	t.Log("[OK 1/3]: Sob carga simultânea, exatamente 1 passageiro reservou o último assento inter-empresas!")

	// PARTE B: Injeção de Falha — Empresa A tem assento livre, mas Empresa B cai no meio da reserva!
	// Nenhum itinerário pode ser confirmado pela metade e o assento da Empresa A deve voltar a ficar livre imediatamente.
	caronasAtuais, _ := utils.CarregarCaronas()
	caronasAtuais[0].Trechos[0].AssentosLivre = 1
	dadosC, _ := json.MarshalIndent(caronasAtuais, "", "  ")
	_ = os.WriteFile(utils.ArquivoCaronas, dadosC, 0644)

	muB.Lock()
	empresaBAtiva = false // Simula queda da Empresa B!
	muB.Unlock()

	_, errMeio := utils.CoordenarReserva2PC("passageiro_falha@email.com", itinInterEmpresas)
	if errMeio == nil {
		t.Fatalf("[FALHA ATOMICIDADE]: A reserva deveria ter abortado com a Empresa B fora do ar!")
	}

	caronasPosFalha, _ := utils.CarregarCaronas()
	if caronasPosFalha[0].Trechos[0].AssentosLivre != 1 {
		t.Fatalf("[FALHA ROLLBACK 2PC]: O assento da Empresa A ficou preso após falha da Empresa B (AssentosLivre=%d, esperava 1)",
			caronasPosFalha[0].Trechos[0].AssentosLivre)
	}
	t.Log("[OK 2/3]: Injeção de falha validada! Queda da Empresa B fez rollback imediato na Empresa A (nenhum itinerário pela metade).")

	// PARTE C: Queda de Coordenador após PREPARE e Recuperação Automática (Desbloqueio de Assento)
	// Simula um coordenador externo que envia PREPARE bloqueando o último assento na Empresa A e cai antes de enviar COMMIT/ABORT.
	coordenadorFantasma := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Quando a Empresa A consultar o status na recuperação, o coordenador informa ABORTED (Presumed Abort)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(utils.Resposta2PC{
			Sucesso: true,
			Estado:  utils.EstadoAborted,
		})
	}))
	defer coordenadorFantasma.Close()

	reqOrfa := utils.Requisicao2PCPrepare{
		TransacaoID:      "TX-ORFA-999",
		ReservaID:        "RES-ORFA-999",
		PassageiroID:     "orfao@email.com",
		EmpresaOrigemID:  "empresaC",
		CoordenadorURL:   coordenadorFantasma.URL,
		TimestampLamport: 500,
		TrechosLocais: []utils.ReferenciaTrechoLocal{{
			CaronaID: "CAR-EMP-A-1",
			Origem:   "Salvador",
			Destino:  "Feira de Santana",
		}},
	}
	bodyPrep, _ := json.Marshal(reqOrfa)
	respPrep, err := http.Post(srvA.URL+"/api/2pc/prepare", "application/json", bytes.NewReader(bodyPrep))
	if err != nil || respPrep.StatusCode != http.StatusOK {
		t.Fatalf("Falha ao enviar PREPARE órfão para Empresa A")
	}
	respPrep.Body.Close()

	// Confirma que o assento foi bloqueado temporariamente pelo PREPARE (AssentosLivre == 0)
	caronasBloqueadas, _ := utils.CarregarCaronas()
	if caronasBloqueadas[0].Trechos[0].AssentosLivre != 0 {
		t.Fatalf("Assento deveria estar bloqueado em 0 durante o estado PREPARED")
	}

	// Executa a rotina de recuperação pós-queda/timeout da Empresa A
	utils.RecuperarTransacoesPendentes()

	caronasRecuperadas, _ := utils.CarregarCaronas()
	if caronasRecuperadas[0].Trechos[0].AssentosLivre != 1 {
		t.Fatalf("[FALHA RECOVERY]: O assento permaneceu bloqueado após a recuperação! AssentosLivre=%d",
			caronasRecuperadas[0].Trechos[0].AssentosLivre)
	}
	t.Log("[OK 3/4]: Recuperação pós-queda validada! Nenhum assento permaneceu bloqueado após falha do coordenador.")

	// PARTE D: Critério de Precedência Distribuída por Relógio Lógico de Lamport entre Servidores Distintos
	// Simula o Servidor C (Lamport=50) segurando o último assento em PREPARED, e logo em seguida chega o
	// Servidor B com uma requisição concorrente de MAIOR precedência (Lamport=10 < 50).
	// O Servidor A deve conceder o assento ao Servidor B (menor Lamport) e abortar a pré-reserva de C!
	reqMenorPrioridade := utils.Requisicao2PCPrepare{
		TransacaoID:      "TX-EMP-C-LENTA",
		ReservaID:        "RES-C-LENTA",
		PassageiroID:     "passageiro_c@email.com",
		EmpresaOrigemID:  "empresaC",
		CoordenadorURL:   "http://empresaC:9083",
		TimestampLamport: 50,
		TrechosLocais: []utils.ReferenciaTrechoLocal{{
			CaronaID: "CAR-EMP-A-1",
			Origem:   "Salvador",
			Destino:  "Feira de Santana",
		}},
	}
	okC, _ := utils.ExecutarPrepareLocal(reqMenorPrioridade)
	if !okC {
		t.Fatalf("Transação inicial de C deveria ter preparado o assento livre")
	}

	reqMaiorPrioridade := utils.Requisicao2PCPrepare{
		TransacaoID:      "TX-EMP-B-PRIORITARIA",
		ReservaID:        "RES-B-PRIO",
		PassageiroID:     "passageiro_b@email.com",
		EmpresaOrigemID:  "empresaB",
		CoordenadorURL:   "http://empresaB:9082",
		TimestampLamport: 10, // Menor Timestamp de Lamport -> Maior Precedência!
		TrechosLocais: []utils.ReferenciaTrechoLocal{{
			CaronaID: "CAR-EMP-A-1",
			Origem:   "Salvador",
			Destino:  "Feira de Santana",
		}},
	}
	okB, _ := utils.ExecutarPrepareLocal(reqMaiorPrioridade)
	if !okB {
		t.Fatalf("[FALHA PRECEDÊNCIA LAMPORT]: A transação de menor Lamport (10 < 50) deveria vencer a disputa pelo último assento!")
	}

	// Quando C tentar dar COMMIT, deve ser rejeitado pois sofreu preempção de Lamport; e B deve conseguir dar COMMIT!
	commitC, _ := utils.ExecutarCommitLocal("TX-EMP-C-LENTA", "RES-C-LENTA")
	if commitC {
		t.Fatalf("[FALHA PRECEDÊNCIA]: A transação perdedora (Lamport 50) não poderia confirmar o assento!")
	}
	commitB, _ := utils.ExecutarCommitLocal("TX-EMP-B-PRIORITARIA", "RES-B-PRIO")
	if !commitB {
		t.Fatalf("[FALHA PRECEDÊNCIA]: A transação vencedora (Lamport 10) deveria confirmar o assento com sucesso!")
	}
	t.Log("[OK 4/4]: Critério de precedência por Relógio Lógico de Lamport entre servidores distintos validado com sucesso!")
}