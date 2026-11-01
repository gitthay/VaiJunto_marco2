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

	// Servidor voltou: reautentica automaticamente a sessão do motorista
	payloadAuth, _ := json.Marshal(map[string]interface{}{
		"email": email,
		"senha": senha,
		"tipo":  utils.TipoMotorista,
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

// função auxiliar para consultar a lista atual de caronas
func buscarCaronas(conexao *net.Conn, serverAddr, email, senha string, buf []byte) ([]utils.Carona, bool) {
	req := utils.MensagemRequisicao{Acao: utils.AcaoConsultarCaronas, Usuario: email}
	resp, ok := enviarRequisicaoTCP(conexao, serverAddr, email, senha, req, buf)
	if !ok {
		return nil, false
	}

	var caronas []utils.Carona
	_ = json.Unmarshal(resp.Payload, &caronas)
	return caronas, true
}

func buscarPassageirosDaCarona(conexao *net.Conn, serverAddr, email, senha, caronaID string, buf []byte) ([]string, bool) {
	payload, _ := json.Marshal(map[string]string{"carona_id": caronaID})
	req := utils.MensagemRequisicao{Acao: utils.AcaoConsultarPassageirosCarona, Usuario: email, Payload: payload}
	resp, ok := enviarRequisicaoTCP(conexao, serverAddr, email, senha, req, buf)
	if !ok {
		return nil, false
	}

	var passageiros []string
	_ = json.Unmarshal(resp.Payload, &passageiros)
	return passageiros, true
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
	fmt.Println("        VAIJUNTO - MOTORISTA              ")
	fmt.Println("==========================================")

	// AUTENTICAÇÃO
	var email, senha string
	for {
		email = lerTexto("Digite seu Email: ")
		senha = lerTexto("Digite sua Senha: ")

		payloadAuth, _ := json.Marshal(map[string]interface{}{
			"email": email,
			"senha": senha,
			"tipo":  utils.TipoMotorista,
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

	//MENU PRINCIPAL
	for {
		fmt.Println("\n------------------------------------------")
		fmt.Println("1. Publicar Nova Carona")
		fmt.Println("2. Listar Minhas Caronas")
		fmt.Println("3. Consultar Passageiros de uma Carona")
		fmt.Println("4. Cancelar Carona")
		fmt.Println("0. Sair")
		fmt.Println("------------------------------------------")

		opcao := lerTexto("Escolha uma opção: ")

		switch opcao {
		case "1":
			fmt.Println("\n--- PUBLICAR CARONA (Digite 0 em qualquer campo para voltar) ---")

			// Leitura e validação da quantidade de assentos
			var assentos int
			cancelarCadastro := false
			for {
				assentosStr := lerTexto("Quantidade total de assentos do veículo: ")
				if assentosStr == "0" {
					cancelarCadastro = true
					break
				}
				val, err := strconv.Atoi(assentosStr)
				if err != nil || val <= 0 {
					fmt.Println("[ERRO]: Quantidade de assentos inválida. Tente novamente.")
					continue
				}
				assentos = val
				break
			}
			if cancelarCadastro {
				continue
			}

			// Leitura e validação da data da viagem
			var dataViagem utils.Data
			var dia, mes, ano int
			for {
				dataStr := lerTexto("Data da viagem (DD/MM/AAAA): ")
				if dataStr == "0" {
					cancelarCadastro = true
					break
				}
				partesData := strings.Split(dataStr, "/")
				if len(partesData) != 3 {
					fmt.Println("[ERRO]: Formato inválido. Use DD/MM/AAAA (ex: 15/10/2026). Tente novamente.")
					continue
				}
				d, errDia := strconv.Atoi(partesData[0])
				m, errMes := strconv.Atoi(partesData[1])
				a, errAno := strconv.Atoi(partesData[2])
				if errDia != nil || errMes != nil || errAno != nil || d < 1 || d > 31 || m < 1 || m > 12 || a < 2024 {
					fmt.Println("[ERRO]: Data inválida ou valores incorretos. Tente novamente.")
					continue
				}
				dia, mes, ano = d, m, a
				dataViagem = utils.NovaData(dia, mes, ano)
				break
			}
			if cancelarCadastro {
				continue
			}

			var trechos []utils.Trecho
			var ultimaCidadeDestino string
			var ultimoHorarioChegada time.Time

			for {
				numeroTrecho := len(trechos) + 1
				fmt.Printf("\n--- Trecho %d ---\n", numeroTrecho)

				// Origem
				var origem string
				if numeroTrecho == 1 {
					for {
						origem = lerTexto("Cidade de Origem: ")
						if origem == "0" {
							cancelarCadastro = true
							break
						}
						if strings.TrimSpace(origem) == "" {
							fmt.Println("[ERRO]: Origem não pode ser vazia. Tente novamente.")
							continue
						}
						break
					}
				} else {
					origem = ultimaCidadeDestino
					fmt.Printf("Cidade de Origem: %s (automática)\n", origem)
				}
				if cancelarCadastro {
					break
				}

				// Destino
				var destino string
				for {
					destino = lerTexto("Cidade de Destino: ")
					if destino == "0" {
						cancelarCadastro = true
						break
					}
					if strings.TrimSpace(destino) == "" {
						fmt.Println("[ERRO]: Destino não pode ser vazio. Tente novamente.")
						continue
					}
					if strings.EqualFold(origem, destino) {
						fmt.Println("[ERRO]: A cidade de destino não pode ser igual à de origem. Tente novamente.")
						continue
					}
					break
				}
				if cancelarCadastro {
					break
				}

				// Horário de Partida
				var horarioPartida time.Time
				if numeroTrecho == 1 {
					for {
						horaPartidaStr := lerTexto("Horário de Partida (HH:MM): ")
						if horaPartidaStr == "0" {
							cancelarCadastro = true
							break
						}

						parsedHora, err := time.Parse("15:04", horaPartidaStr)
						if err != nil {
							fmt.Println("[ERRO]: Formato de horário inválido. Use HH:MM (ex: 08:30). Tente novamente.")
							continue
						}

						horarioPartida = time.Date(ano, time.Month(mes), dia, parsedHora.Hour(), parsedHora.Minute(), 0, 0, time.Local)
						break
					}
				} else {
					// Automaticamente igual à chegada do trecho anterior
					horarioPartida = ultimoHorarioChegada
					fmt.Printf("Horário de Partida: %s (automático do trecho anterior)\n", horarioPartida.Format("15:04"))
				}
				if cancelarCadastro {
					break
				}

				// Horário de Chegada
				var horarioChegada time.Time
				for {
					horaChegadaStr := lerTexto("Horário de Chegada (HH:MM): ")
					if horaChegadaStr == "0" {
						cancelarCadastro = true
						break
					}

					parsedHora, err := time.Parse("15:04", horaChegadaStr)
					if err != nil {
						fmt.Println("[ERRO]: Formato de horário inválido. Use HH:MM (ex: 10:00). Tente novamente.")
						continue
					}

					horarioChegada = time.Date(ano, time.Month(mes), dia, parsedHora.Hour(), parsedHora.Minute(), 0, 0, time.Local)

					if !horarioChegada.After(horarioPartida) {
						fmt.Printf("[ERRO]: Equívoco no horário! A chegada (%s) deve ser após a partida (%s). Tente novamente.\n",
							horarioChegada.Format("15:04"), horarioPartida.Format("15:04"))
						continue
					}
					break
				}
				if cancelarCadastro {
					break
				}

				// Preço
				var preco float64
				for {
					precoStr := lerTexto("Preço deste trecho (R$): ")
					if precoStr == "0" {
						cancelarCadastro = true
						break
					}

					p, err := strconv.ParseFloat(strings.ReplaceAll(precoStr, ",", "."), 64)
					if err != nil || p <= 0 {
						fmt.Println("[ERRO]: Preço inválido. Digite um valor positivo numérico. Tente novamente.")
						continue
					}
					preco = p
					break
				}
				if cancelarCadastro {
					break
				}

				// Registra o trecho
				trechos = append(trechos, utils.Trecho{
					Origem:         origem,
					Destino:        destino,
					DataPartida:    dataViagem,
					HorarioPartida: horarioPartida,
					HorarioChegada: horarioChegada,
					Preco:          preco,
					AssentosLivre:  assentos,
				})

				ultimaCidadeDestino = destino
				ultimoHorarioChegada = horarioChegada

				maisTrecho := lerTexto("Deseja adicionar mais um trecho a esta carona? (s/n): ")
				if strings.ToLower(maisTrecho) != "s" {
					break
				}
			}

			if cancelarCadastro || len(trechos) == 0 {
				fmt.Println("\nPublicação cancelada.")
				continue
			}

			novaCarona := utils.Carona{
				ID:          fmt.Sprintf("CAR-%d", time.Now().UnixNano()),
				MotoristaID: email,
				AssentosTot: assentos,
				Ativa:       true,
				Trechos:     trechos,
			}

			payload, _ := json.Marshal(novaCarona)
			req := utils.MensagemRequisicao{Acao: utils.AcaoPublicarCarona, Usuario: email, Payload: payload}
			resp, ok := enviarRequisicaoTCP(&conexao, *serverAddr, email, senha, req, buf)
			if !ok {
				continue
			}
			fmt.Println("\n>", resp.Mensagem)

		case "2":
			caronas, ok := buscarCaronas(&conexao, *serverAddr, email, senha, buf)
			if !ok {
				continue
			}
			fmt.Println("\n==========================================")
			fmt.Println("           MINHAS CARONAS                 ")
			fmt.Println("==========================================")

			if len(caronas) == 0 {
				fmt.Println("Nenhuma carona encontrada.")
				continue
			}

			var ativas []utils.Carona
			var naoAtivas []utils.Carona

			for _, c := range caronas {
				if c.Ativa {
					ativas = append(ativas, c)
				} else {
					naoAtivas = append(naoAtivas, c)
				}
			}

			// Print de Caronas Ativas
			fmt.Println("\n>>> CARONAS ATIVAS <<<")
			if len(ativas) == 0 {
				fmt.Println("Nenhuma carona ativa no momento.")
			} else {
				for i, c := range ativas {
					fmt.Printf("\n[%d] ID: %s | Assentos Totais: %d\n", i+1, c.ID, c.AssentosTot)
					fmt.Println("  Trechos:")
					for idx, t := range c.Trechos {
						fmt.Printf("    Trecho %d: %s -> %s | Data: %s | Horário: %s às %s | Preço: R$ %.2f | Assentos Livres: %d\n",
							idx+1, t.Origem, t.Destino, t.DataPartida.String(),
							t.HorarioPartida.Format("15:04"), t.HorarioChegada.Format("15:04"),
							t.Preco, t.AssentosLivre)
					}
				}
			}

			// Print de Caronas Não Ativas / Canceladas
			fmt.Println("\n>>> CARONAS NÃO ATIVAS (CANCELADAS) <<<")
			if len(naoAtivas) == 0 {
				fmt.Println("Nenhuma carona cancelada.")
			} else {
				for i, c := range naoAtivas {
					fmt.Printf("\n[%d] ID: %s | Assentos Totais: %d\n", i+1, c.ID, c.AssentosTot)
					fmt.Println("  Trechos:")
					for idx, t := range c.Trechos {
						fmt.Printf("    Trecho %d: %s -> %s | Data: %s | Horário: %s às %s | Preço: R$ %.2f\n",
							idx+1, t.Origem, t.Destino, t.DataPartida.String(),
							t.HorarioPartida.Format("15:04"), t.HorarioChegada.Format("15:04"),
							t.Preco)
					}
				}
			}

		case "3":
			caronas, ok := buscarCaronas(&conexao, *serverAddr, email, senha, buf)
			if !ok {
				continue
			}
			fmt.Println("\n==========================================")
			fmt.Println("    CONSULTAR PASSAGEIROS DA CARONA       ")
			fmt.Println("==========================================")

			if len(caronas) == 0 {
				fmt.Println("Nenhuma carona encontrada.")
				continue
			}

			for i, c := range caronas {
				origemGeral := c.Trechos[0].Origem
				destinoGeral := c.Trechos[len(c.Trechos)-1].Destino
				status := "Ativa"
				if !c.Ativa {
					status = "Cancelada"
				}
				fmt.Printf("[%d] ID: %s | Rota: %s -> %s (%s)\n", i+1, c.ID, origemGeral, destinoGeral, status)
			}

			fmt.Println("\n[0] Voltar ao menu")
			input := lerTexto("Escolha o número da carona para ver os passageiros: ")
			num, err := strconv.Atoi(input)
			if err != nil || num == 0 || num > len(caronas) {
				continue
			}

			caronaEscolhida := caronas[num-1]
			passageiros, ok := buscarPassageirosDaCarona(&conexao, *serverAddr, email, senha, caronaEscolhida.ID, buf)
			if !ok {
				continue
			}

			fmt.Printf("\n--- Passageiros da Carona %s ---\n", caronaEscolhida.ID)
			if len(passageiros) == 0 {
				fmt.Println("Nenhum passageiro reservou esta carona ainda.")
			} else {
				for _, p := range passageiros {
					fmt.Printf("- %s\n", p)
				}
			}

		case "4":
			caronas, ok := buscarCaronas(&conexao, *serverAddr, email, senha, buf)
			if !ok {
				continue
			}
			fmt.Println("\n==========================================")
			fmt.Println("           CANCELAR CARONA                ")
			fmt.Println("==========================================")

			if len(caronas) == 0 {
				fmt.Println("Nenhuma carona encontrada.")
				continue
			}

			// Filtra apenas as ativas
			var ativasParaCancelar []utils.Carona
			for _, c := range caronas {
				if c.Ativa {
					ativasParaCancelar = append(ativasParaCancelar, c)
				}
			}

			if len(ativasParaCancelar) == 0 {
				fmt.Println("Não há caronas ativas disponíveis para cancelamento.")
				continue
			}

			// Exibe detalhes completos de cada carona ativa
			abortouPorQueda := false
			for i, c := range ativasParaCancelar {
				origemGeral := "Indefinida"
				destinoGeral := "Indefinido"
				dataStr := "Indefinida"
				if len(c.Trechos) > 0 {
					origemGeral = c.Trechos[0].Origem
					destinoGeral = c.Trechos[len(c.Trechos)-1].Destino
					dataStr = c.Trechos[0].DataPartida.String()
				}

				passageiros, okP := buscarPassageirosDaCarona(&conexao, *serverAddr, email, senha, c.ID, buf)
				if !okP {
					abortouPorQueda = true
					break
				}

				fmt.Printf("\n[%d] Rota: %s -> %s | Data: %s | Trechos: %d\n",
					i+1, origemGeral, destinoGeral, dataStr, len(c.Trechos))

				if len(passageiros) == 0 {
					fmt.Println("    Passageiros: Nenhum passageiro reservou esta carona.")
				} else {
					fmt.Printf("    Passageiros (%d): %s\n", len(passageiros), strings.Join(passageiros, ", "))
				}
			}
			if abortouPorQueda {
				continue
			}

			fmt.Println("\n[0] Voltar ao menu")

			input := lerTexto("Escolha o número da carona para cancelar: ")
			num, err := strconv.Atoi(input)
			if err != nil || num == 0 || num > len(ativasParaCancelar) {
				continue
			}

			caronaEscolhida := ativasParaCancelar[num-1]
			payload, _ := json.Marshal(map[string]interface{}{"carona_id": caronaEscolhida.ID, "confirmar": false})
			req := utils.MensagemRequisicao{Acao: utils.AcaoCancelarCarona, Usuario: email, Payload: payload}
			resp, ok := enviarRequisicaoTCP(&conexao, *serverAddr, email, senha, req, buf)
			if !ok {
				continue
			}

			fmt.Println("\n>", resp.Mensagem)

			if !resp.Sucesso && strings.Contains(resp.Mensagem, "ATENÇÃO") {
				confirmar := lerTexto("Deseja realmente cancelar? (s/n ou 0 para voltar): ")
				if strings.ToLower(confirmar) == "s" {
					payloadConf, _ := json.Marshal(map[string]interface{}{"carona_id": caronaEscolhida.ID, "confirmar": true})
					req.Payload = payloadConf
					resp, ok = enviarRequisicaoTCP(&conexao, *serverAddr, email, senha, req, buf)
					if !ok {
						continue
					}
					fmt.Println("\n>", resp.Mensagem)
				}
			}

		case "0":
			fmt.Println("Saindo...")
			return
		}
	}
}
