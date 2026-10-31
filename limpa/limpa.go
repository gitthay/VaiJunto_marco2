package main

import (
	"fmt"
	"os"
	"path/filepath"
)

func main() {
	diretorioDados := "./dados"

	// Verifica se a pasta dados existe
	if _, err := os.Stat(diretorioDados); os.IsNotExist(err) {
		fmt.Println("[AVISO] A pasta 'dados' não foi encontrada.")
		return
	}

	// Lê o conteúdo da pasta dados
	empresas, err := os.ReadDir(diretorioDados)
	if err != nil {
		fmt.Printf("[ERRO] Não foi possível ler a pasta dados: %v\n", err)
		return
	}

	arquivosAlvo := []string{"caronas.json", "reservas.json", "usuarios.json"}

	for _, empresa := range empresas {
		if empresa.IsDir() {
			pastaEmpresa := filepath.Join(diretorioDados, empresa.Name())
			fmt.Printf("Limpando dados da pasta: %s\n", pastaEmpresa)

			for _, nomeArquivo := range arquivosAlvo {
				caminhoArquivo := filepath.Join(pastaEmpresa, nomeArquivo)
				
				// Sobrescreve o arquivo com um array JSON vazio "[]"
				err := os.WriteFile(caminhoArquivo, []byte("[]"), 0644)
				if err != nil {
					fmt.Printf("  -> Erro ao limpar %s: %v\n", nomeArquivo, err)
				} else {
					fmt.Printf("  -> %s limpo com sucesso.\n", nomeArquivo)
				}
			}
		}
	}

	fmt.Println("\nLimpeza de todos os arquivos JSON concluída com sucesso!")
}