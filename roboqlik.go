package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	"github.com/kr/fs"
	"github.com/tealeg/xlsx"
)

type agrupador struct {
	Agrupa []agrupa `json:"agrupador,omitempty"`
}

type agrupa struct {
	Agrupador  string `json:"agrupador,omitempty"`
	Dicionario []dicionario
}

type arquivoJson struct {
	Arquivo []arquivo `json:"estrutura,omitempty"`
}
type arquivo struct {
	Empresa       string       `json:"empresa,omitempty"`
	Agrupador     string       `json:"agrupador,omitempty"`
	NomeArquivo   string       `json:"nomearquivo,omitempty"`
	CaminhoFisico string       `json:"caminhofisico,omitempty"`
	Dicionario    []dicionario `json:"dicionario,omitempty"`
}
type dicionario struct {
	Sheet       string `json:"sheet,omitempty"`
	De          string `json:"de,omitempty"`
	Para        string `json:"para,omitempty"`
	Tipo        string `json:"tipo,omitempty"`
	Obrigatorio string `json:"obrigatorio,omitempty"`
	Empresa     string `json:"empresa,omitempty"`
}

type Config struct {
	Configuracao configuracao `json:"configuracao"`
}
type configuracao struct {
	Criardiretorio   string      `json:"criardiretorio"`
	ExecucaoContinua string      `json:"execucaocontinua"`
	TempoDeExecucao  int         `json:"tempodeexecucao"`
	EnviarEmail      string      `json:"enviaremail"`
	Diretorios       diretorios  `json:"diretorios"`
	Metadados        metadados   `json:"metadados"`
	Email            emailconfig `json:"email"`
}

type diretorios struct {
	CSVGerados           string `json:"csvgerados"`
	PlanilhasImportadas  string `json:"planilhasimportadas"`
	PlanilhasAImportar   string `json:"planilhasaimportar"`
	PlanilhasComErro     string `json:"planilhascomerro"`
	PlanilhasSemMetaDado string `json:"planilhasSemMetaDado"`
	Log                  string `json:"log"`
}

type metadados struct {
	Diretorio   string `json:"diretorio"`
	NomeArquivo string `json:"nomearquivo"`
}

type emailconfig struct {
	NomeRemetente string `json:"nomeremetende"`
	Titulo        string `json:"titulo"`
	Mensagem      string `json:"mensagem"`
	Servidor      string `json:"servidor"`
	Porta         int    `json:"porta"`
	ContaEmail    string `json:"contaemail"`
	Senha         string `json:"senha"`
}

var (
	dic        map[string][]string
	est        map[string][]*dicionario
	email      map[string][]string
	wg         sync.WaitGroup
	tasks      chan string
	config     Config
	dicArquivo map[string]string
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var memprofile = flag.String("memprofile", "", "write memory profile to this file")

func main() {
	flag.Parse()

	if validaVersao() {

		//profile de cpu
		if *cpuprofile != "" {
			f, err := os.Create(*cpuprofile)
			if err != nil {
				log.Fatal(err)
			}
			pprof.StartCPUProfile(f)
			defer pprof.StopCPUProfile()
		}

		tasks = make(chan string, 50)

		carregarConfiguracao()
		criarFilaProcessamento(4)

		dic, est, email, dicArquivo = carregaDicionario()
		carregarArquivoNaFilaWalk()

		// profile de memoria
		if *memprofile != "" {
			fm, err := os.Create(*memprofile)
			if err != nil {
				log.Fatal(err)
			}
			pprof.WriteHeapProfile(fm)
			fm.Close()
		}
	}
}

func validaVersao() bool {
	t1 := time.Now()
	t2 := time.Date(2016, 5, 10, 12, 0, 0, 0, time.UTC)
	if t2.Before(t1) {
		fmt.Println("Robo com erro interno. Impossível realizar as validações. Entre em contato...")
		return false
	} else {
		return true
	}
}

func carregarConfiguracao() {
	dat, err := ioutil.ReadFile("config.json")
	if err != nil {
		fmt.Println("Erro ao carregar a configuração.")
		panic(err.Error())
	}
	err = json.Unmarshal(dat, &config)
	if err != nil {
		fmt.Println("Erro ao carregar a configuração.")
		panic(err.Error())
	}

	if config.Configuracao.Criardiretorio == "S" {
		os.MkdirAll(config.Configuracao.Diretorios.CSVGerados, os.ModeType)
		os.MkdirAll(config.Configuracao.Diretorios.Log, os.ModeType)
		os.MkdirAll(config.Configuracao.Diretorios.PlanilhasAImportar, os.ModeType)
		os.MkdirAll(config.Configuracao.Diretorios.PlanilhasComErro, os.ModeType)
		os.MkdirAll(config.Configuracao.Diretorios.PlanilhasImportadas, os.ModeType)
		os.MkdirAll(config.Configuracao.Diretorios.PlanilhasSemMetaDado, os.ModeType)
	}
}

func criarFilaProcessamento(numCPU int) {
	for i := 0; i < numCPU; i++ {
		go func(cpu int) {
			for arq := range tasks {
				wg.Add(1)
				fmt.Println("cpu: ", cpu, " ", arq)
				cmd := exec.Command("ls")
				//cmd := exec.Command(".\\roboexcel.exe ", fmt.Sprintf("-nomearquivo=%q", arq))
				fmt.Println(cmd.Path, cmd.Args)
				err := cmd.Start() //err := cmd.Run()
				err = cmd.Wait()
				if err != nil {
				}
				//fmt.Println("Erro ao esperar a Interpretação do arquivo - ", err.Error())
				/*
					//err := cmd.Start()
					if err != nil {
						fmt.Println("Erro ao Interpretar o arquivo - ", err.Error())
					} else {
						err = cmd.Wait()
						if err != nil {
							fmt.Println("Erro ao esperar a Interpretação do arquivo - ", err.Error())
						}
						wg.Done()
					}
				*/
				wg.Done()
			}
		}(i)
	}
}

func carregarArquivoNaFilaWalk() {
	dirname := config.Configuracao.Diretorios.PlanilhasAImportar + "\\"
	for {

		walker := fs.Walk(dirname)
		for walker.Step() {
			if err := walker.Err(); err != nil {
				fmt.Fprintln(os.Stderr, err)
				continue
			}
			info := walker.Stat()
			if !info.IsDir() {
				if fmt.Sprintf(".\\%s", walker.Path()) != fmt.Sprintf("%s\\%s", config.Configuracao.Diretorios.PlanilhasAImportar, info.Name()) {
					erro := os.Rename(fmt.Sprintf(".\\%s", walker.Path()), fmt.Sprintf("%s\\%s", config.Configuracao.Diretorios.PlanilhasAImportar, info.Name()))
					if erro != nil {
						fmt.Println(erro.Error())
					}
				}
				fmt.Println(info.Name())
				tasks <- info.Name()
			}
		}
		if config.Configuracao.ExecucaoContinua != "S" {
			wg.Wait()
			break
		}

		fmt.Println(fmt.Sprintf("---   %d segundos...   ---", config.Configuracao.TempoDeExecucao))
		time.Sleep(time.Duration(config.Configuracao.TempoDeExecucao) * time.Second)

		wg.Wait()
	}
	close(tasks)
}

func carregarArquivoNaFila() {

	dirname := config.Configuracao.Diretorios.PlanilhasAImportar + "\\"
	d, err := os.Open(dirname)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	defer d.Close()
	for {
		files, err := d.Readdir(-1)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}
		for _, file := range files {
			if file.Mode().IsRegular() {
				tasks <- file.Name()
			}
		}
		if config.Configuracao.ExecucaoContinua != "S" {
			break
		}
		time.Sleep(time.Duration(config.Configuracao.TempoDeExecucao) * time.Second)
		fmt.Println(fmt.Sprintf("---   %d segundos...   ---", config.Configuracao.TempoDeExecucao))
	}
	close(tasks)
	wg.Wait()
}

func carregaDicionario() (map[string][]string, map[string][]*dicionario, map[string][]string, map[string]string) {
	excelFileName := config.Configuracao.Metadados.Diretorio + "\\" + config.Configuracao.Metadados.NomeArquivo
	//fmt.Println("metadado: ", excelFileName)

	xlFile, err := xlsx.OpenFile(excelFileName)
	if err != nil {
		fmt.Println(fmt.Sprintf("Erro ao abrir o arquivo [%s]. \n Salvar o arquivo no formato xlsx, provavelmente o arquivo esta no formato antigo xls. \n%s", excelFileName, err.Error()))
		panic(err.Error())
	}

	var empresa map[string][]string
	empresa = make(map[string][]string)

	var agrupador map[string][]*dicionario
	agrupador = make(map[string][]*dicionario)

	var email map[string][]string
	email = make(map[string][]string)

	var dicArq map[string]string
	dicArq = make(map[string]string)

	var (
		emp string
		agr string
		//cam     string
		plan    string
		nomeArq string
		de      string
		para    string
		obr     string
		tipo    string
		email1  string
		email2  string
		email3  string
	)

	sheet := xlFile.Sheets[0]
	for i, row := range sheet.Rows {
		if len(row.Cells) > 0 && i > 0 {

			emp = strings.ToLower(row.Cells[0].Value)
			agr = strings.ToLower(row.Cells[1].Value)
			nomeArq = strings.ToLower(row.Cells[2].Value)
			plan = strings.ToLower(row.Cells[3].Value)
			//cam = strings.ToLower(row.Cells[4].Value)
			de = strings.ToLower(row.Cells[5].Value)
			para = row.Cells[6].Value
			obr = strings.ToLower(row.Cells[7].Value)

			if len(row.Cells) > 8 {
				tipo = strings.ToLower(row.Cells[8].Value)
			}
			arq1 := strings.Split(nomeArq, "*")
			arq1_0 := strings.Replace(arq1[0], "–", "-", -1)
			if strings.TrimSpace(arq1[0]) != "" {
				ind := fmt.Sprintf("%s|%s", arq1_0, plan)
				_, ok := dicArq[arq1_0]
				if !ok {
					dicArq[arq1_0] = ind
				}
				_, ok = empresa[ind]
				if !ok {
					empdic := []string{emp, agr, plan}
					empresa[ind] = append(empresa[ind], empdic...)
				}

				agdic := dicionario{plan, de, para, tipo, obr, emp}
				agrupador[agr] = append(agrupador[agr], &agdic)
			}

		}
	}

	sheet = xlFile.Sheets[1]
	for i, row := range sheet.Rows {
		if len(row.Cells) > 0 && i > 0 {

			emp = strings.ToLower(row.Cells[0].Value)
			nomeArq = strings.ToLower(row.Cells[1].Value)
			if len(row.Cells) > 2 {
				email1 = strings.ToLower(row.Cells[2].Value)
			}
			if len(row.Cells) > 3 {
				email2 = strings.ToLower(row.Cells[3].Value)
			}
			if len(row.Cells) > 4 {
				email3 = strings.ToLower(row.Cells[4].Value)
			}

			_, ok := email[nomeArq]
			if !ok {
				empdic := []string{emp, email1, email2, email3}
				email[nomeArq] = append(email[nomeArq], empdic...)
			}
		}
	}

	jsonString, err := json.MarshalIndent(agrupador, "", "\t")
	if err != nil {
		jsonString = []byte("erro: " + err.Error())
	}
	err = ioutil.WriteFile(config.Configuracao.Metadados.Diretorio+"\\DicionarioAgrupador.json", jsonString, os.ModeType)
	if err != nil {
		panic(err.Error())
	}

	jsonString, err = json.MarshalIndent(empresa, "", "\t")
	if err != nil {
		jsonString = []byte("erro: " + err.Error())
	}
	err = ioutil.WriteFile(config.Configuracao.Metadados.Diretorio+"\\DicionarioEmpresa.json", jsonString, os.ModeType)
	if err != nil {
		panic(err.Error())
	}

	jsonString, err = json.MarshalIndent(dicArq, "", "\t")
	if err != nil {
		jsonString = []byte("erro: " + err.Error())
	}
	err = ioutil.WriteFile(config.Configuracao.Metadados.Diretorio+"\\DicionarioArquivo.json", jsonString, os.ModeType)
	if err != nil {
		panic(err.Error())
	}

	jsonString, err = json.MarshalIndent(email, "", "\t")
	if err != nil {
		jsonString = []byte("erro: " + err.Error())
	}
	err = ioutil.WriteFile(config.Configuracao.Metadados.Diretorio+"\\DicionarioEmail.json", jsonString, os.ModeType)
	if err != nil {
		panic(err.Error())
	}

	return empresa, agrupador, email, dicArq

}
