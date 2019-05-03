package main

import (
	"crypto/tls"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	"github.com/kr/fs"
	"github.com/tealeg/xlsx"
	"gopkg.in/gomail.v2"
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

func geraArquivoCSV(logger *log.Logger, nome string, plan [][]string, arq string, nomesheet string, nomeAgrupador string, nomeEmpresa string) {
	//fmt.Println(fmt.Sprintf(" CSV - sheet=%s / nome=%s / nomeEmpresa=%s / nomeAgrupador=%s:", nomesheet, nome, nomeEmpresa, nomeAgrupador))
	if len(plan) > 0 {
		file, err := os.Create(fmt.Sprintf("%s\\%s_%s%s.csv", config.Configuracao.Diretorios.CSVGerados, nomeAgrupador, nomesheet, nome))
		if err != nil {
			return
		}
		defer file.Close()

		w := csv.NewWriter(file)
		w.WriteAll(plan)
		if err := w.Error(); err != nil {
		}
		fmt.Println("sem erro: ", fmt.Sprintf("%s\\%s", config.Configuracao.Diretorios.PlanilhasImportadas, nome))
		os.Rename(fmt.Sprintf("%s\\%s.xlsx", config.Configuracao.Diretorios.PlanilhasAImportar, nome), fmt.Sprintf("%s\\%s.xlsx", config.Configuracao.Diretorios.PlanilhasImportadas, nome))
		os.Rename(fmt.Sprintf("%s\\%s.xlsm", config.Configuracao.Diretorios.PlanilhasAImportar, nome), fmt.Sprintf("%s\\%s.xlsm", config.Configuracao.Diretorios.PlanilhasImportadas, nome))
		os.Rename(fmt.Sprintf("%s\\%s.xls", config.Configuracao.Diretorios.PlanilhasAImportar, nome), fmt.Sprintf("%s\\%s.xls", config.Configuracao.Diretorios.PlanilhasImportadas, nome))
		os.Rename(fmt.Sprintf("%s\\%s", config.Configuracao.Diretorios.PlanilhasAImportar, nome), fmt.Sprintf("%s\\%s", config.Configuracao.Diretorios.PlanilhasSemMetaDado, nome))
		os.Remove(fmt.Sprintf("%s\\%s.log", config.Configuracao.Diretorios.Log, nome))
	} else {
		if nomesheet == "" {
			fmt.Println("ERRO: ", fmt.Sprintf("%s\\%s", config.Configuracao.Diretorios.PlanilhasAImportar, nome))
			os.Rename(fmt.Sprintf("%s\\%s.xlsx", config.Configuracao.Diretorios.PlanilhasAImportar, nome), fmt.Sprintf("%s\\%s.xlsx", config.Configuracao.Diretorios.PlanilhasComErro, nome))
			os.Rename(fmt.Sprintf("%s\\%s.xlsm", config.Configuracao.Diretorios.PlanilhasAImportar, nome), fmt.Sprintf("%s\\%s.xlsm", config.Configuracao.Diretorios.PlanilhasComErro, nome))
			os.Rename(fmt.Sprintf("%s\\%s.xls", config.Configuracao.Diretorios.PlanilhasAImportar, nome), fmt.Sprintf("%s\\%s.xls", config.Configuracao.Diretorios.PlanilhasComErro, nome))
			os.Rename(fmt.Sprintf("%s\\%s", config.Configuracao.Diretorios.PlanilhasAImportar, nome), fmt.Sprintf("%s\\%s", config.Configuracao.Diretorios.PlanilhasSemMetaDado, nome))
			os.Rename(fmt.Sprintf("%s\\%s.log", config.Configuracao.Diretorios.Log, nome), fmt.Sprintf("%s\\%s_%s.log", config.Configuracao.Diretorios.PlanilhasComErro, nome, nomesheet))
			aux := strings.Split(arq, "|")
			enviarEmail(aux[0], nome)
		} else {
			if nomeEmpresa != "" {
				nomeEmpresa = nomeEmpresa + "_"
			}
			fmt.Println("SemMetaDado: ", fmt.Sprintf("%s\\%s%s", config.Configuracao.Diretorios.PlanilhasAImportar, nomeEmpresa, nome))
			os.Rename(fmt.Sprintf("%s\\%s.xlsx", config.Configuracao.Diretorios.PlanilhasAImportar, nome), fmt.Sprintf("%s\\%s%s.xlsx", config.Configuracao.Diretorios.PlanilhasSemMetaDado, nomeEmpresa, nome))
			os.Rename(fmt.Sprintf("%s\\%s.xlsm", config.Configuracao.Diretorios.PlanilhasAImportar, nome), fmt.Sprintf("%s\\%s%s.xlsm", config.Configuracao.Diretorios.PlanilhasSemMetaDado, nomeEmpresa, nome))
			os.Rename(fmt.Sprintf("%s\\%s.xls", config.Configuracao.Diretorios.PlanilhasAImportar, nome), fmt.Sprintf("%s\\%s%s.xls", config.Configuracao.Diretorios.PlanilhasSemMetaDado, nomeEmpresa, nome))
			os.Rename(fmt.Sprintf("%s\\%s", config.Configuracao.Diretorios.PlanilhasAImportar, nome), fmt.Sprintf("%s\\%s%s", config.Configuracao.Diretorios.PlanilhasSemMetaDado, nomeEmpresa, nome))
			os.Remove(fmt.Sprintf("%s\\%s.log", config.Configuracao.Diretorios.Log, nome))
		}
	}
}

func interpretarPlanilha(arq []string, cpu int) {

	defer wg.Done()

	auxname := strings.Split(arq[1], ".xls")
	file, err := os.Create(fmt.Sprintf("%s\\%s.log", config.Configuracao.Diretorios.Log, auxname[0]))
	if err != nil {
		fmt.Println("create log: ", err.Error())
		return
	}
	defer file.Close()

	logger := log.New(file, "", log.Ldate+log.Ltime)
	logger.SetOutput(file)

	erroarq := false
	var xlFile *xlsx.File

	nomeArq := fmt.Sprintf("%s\\%s", config.Configuracao.Diretorios.PlanilhasAImportar, arq[1])

	defer func() {
		if r := recover(); r != nil {
			fmt.Println("log interp...")

			logger.Println(fmt.Sprintf("Erro ao abrir o arquivo [%s]. O Arquivo não esta no formato correto. %s", nomeArq, r))
			file.Close()
			geraArquivoCSV(logger, strings.ToLower(arq[1]), [][]string{}, arq[2], "", "", "")

		}
	}()

	if strings.Contains(arq[1], ".xls") {
		xlFile, err = xlsx.OpenFile(nomeArq)
		if err != nil {
			logger.Println(fmt.Sprintf("Erro ao abrir o arquivo [%s]. \n O Arquivo não esta no formato correto. %s", nomeArq, err.Error()))
			erroarq = true
		}
	} else {
		erroarq = true
	}

	var auxPlan map[string][][]string
	auxPlan = make(map[string][][]string)
	var agrup string
	var auxemp string
	if !erroarq {
		for _, sheet := range xlFile.Sheets {
			plan := carregaPlan(logger, arq, sheet)
			emp, ok := dic[arq[2]+"|"+strings.ToLower(sheet.Name)]
			agrup = ""
			auxemp = ""
			if ok {
				agrup = emp[1]
				auxemp = emp[0]
			}
			auxPlan[auxemp+"|"+agrup+"|"+sheet.Name+"_"] = plan

		}
	}
	auxemp = ""
	auxarq, okarq := dicArquivo[arq[2]]
	if okarq {
		emp, ok := dic[auxarq]
		if ok {
			auxemp = emp[0]
		}
	}

	auxFile, errF := file.Stat()
	if errF != nil {
	}

	if auxFile.Size() > 0 {
		auxname := strings.Split(arq[1], ".xls")
		geraArquivoCSV(logger, strings.ToLower(auxname[0]), [][]string{}, arq[2], "", "", auxemp)
	} else {
		vazio := true
		for k, v := range auxPlan {
			if len(v) > 0 {
				aux := strings.Split(k, "|")
				geraArquivoCSV(logger, strings.ToLower(strings.Replace(strings.Replace(strings.Replace(arq[1], ".xlsx", "", -1), ".xlsm", "", -1), ".xls", "", -1)), v, arq[2], aux[2], aux[1], aux[0])
				vazio = false
			}
		}
		if vazio {
			geraArquivoCSV(logger, strings.ToLower(strings.Replace(strings.Replace(strings.Replace(arq[1], ".xlsx", "", -1), ".xlsm", "", -1), ".xls", "", -1)), [][]string{}, arq[2], "_", "", auxemp)
		}
	}
}

func carregaPlan(logger *log.Logger, arq []string, sheet *xlsx.Sheet) [][]string {
	var agr string
	var plan [][]string
	var linha []string
	cabecalho := "|"
	cabecalhoDe := "|"

	var complemento []string

	excluir := make(map[int]bool)
	interromper := false

	emp, ok := dic[arq[2]+"|"+strings.ToLower(sheet.Name)]
	if ok {
		agr = emp[1]
		for i, row := range sheet.Rows {
			linha = []string{}

			for j, cels := range row.Cells {
				conteudo := cels.Value

				if i == 0 { //trata o cabeçalho
					ag, agBool := est[agr]
					if agBool {
						achei := false
						for _, cab := range ag {
							if cab.De == strings.ToLower(conteudo) && !strings.Contains(cabecalho, "|"+cab.Para+"|") && cab.Sheet == strings.ToLower(sheet.Name) && cab.Empresa == emp[0] {
								conteudo = cab.Para
								cabecalho = cabecalho + cab.Para + "|"
								achei = true
								break
							}
						}
						if !achei {
							excluir[j] = true
						}
					}
				}

				_, ok := excluir[j]
				if !ok {
					linha = append(linha, conteudo)
				}
			}

			if i == 0 {
				for _, cab := range est[agr] {
					if emp[0] == cab.Empresa {
						if !strings.Contains(cabecalho, "|"+cab.Para+"|") {
							if cab.Obrigatorio == "s" {
								if !strings.Contains(cabecalhoDe, "|"+cab.De+"|") {
									logger.Println(fmt.Sprintf("[plan: %s] - A coluna [%s] é obrigatório e não se encontra na planilha. Verifique o dicionário de dados deste arquivo.\n\r", sheet.Name, cab.De))
									cabecalhoDe = cabecalhoDe + cab.De + "|"
								}
								interromper = true
							} else {
								if cab.Obrigatorio != "o" && cab.Obrigatorio != "n" {
									linha = append(linha, cab.Para)
									aux := ""
									if cab.Tipo == "n" {
										aux = "0"
									}
									complemento = append(complemento, aux)
								}
							}
						}
					}
				}
				linha = append(linha, "idempresa")
			} else {
				for _, x := range complemento {
					linha = append(linha, x)
				}
				linha = append(linha, emp[0])
			}

			if interromper {
				return [][]string{}
			}

			plan = append(plan, linha)
		}
	}
	return plan
}

func criarFilaProcessamento(numCPU int) {
	for i := 0; i < numCPU; i++ {
		go func(cpu int) {
			for arq := range tasks {
				wg.Add(1)
				interpretarPlanilha(buscarNomeArquivo(strings.ToLower(arq)), cpu)
			}
		}(i)
	}
}

func buscarNomeArquivo(arq string) []string {
	if strings.Contains(arq, ".xls") {
		aux := strings.Replace(arq, "–", "-", -1)
		aux = strings.TrimSpace(strings.Replace(strings.Replace(strings.Replace(arq, ".xlsx", "", -1), ".xlsm", "", -1), ".xls", "", -1))

		for k, v := range dicArquivo {
			if strings.Contains(aux, strings.Replace(k, "–", "-", -1)) {
				return []string{strings.Replace(v, "–", "-", -1), strings.Replace(arq, "–", "-", -1), strings.Replace(k, "–", "-", -1)}
			}
		}
	}
	return []string{strings.Replace(arq, "–", "-", -1), strings.Replace(arq, "–", "-", -1), strings.Replace(arq, "–", "-", -1)}
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
				fmt.Println(info.IsDir(), info.Name())
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

func enviarEmail(arq string, arq1 string) {
	if config.Configuracao.EnviarEmail != "S" {
		return
	} else {
		m := gomail.NewMessage()
		m.SetHeader("From", config.Configuracao.Email.ContaEmail)
		m.SetHeader("To", email[arq][1])
		m.SetHeader("Subject", config.Configuracao.Email.Titulo)
		m.SetBody("text/html", fmt.Sprintf("Planilha: %s. \r\n %s", arq, config.Configuracao.Email.Mensagem))
		m.Attach(fmt.Sprintf("%s\\%s.log", config.Configuracao.Diretorios.PlanilhasComErro, arq))
		d := gomail.NewDialer(config.Configuracao.Email.Servidor, config.Configuracao.Email.Porta, config.Configuracao.Email.ContaEmail, config.Configuracao.Email.Senha)
		d.TLSConfig = &tls.Config{InsecureSkipVerify: true}
		if err := d.DialAndSend(m); err != nil {
			//panic(err)
		}
	}
}
