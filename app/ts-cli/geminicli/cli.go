// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package geminicli

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/influxdata/influxdb/client"
	"github.com/influxdata/influxdb/models"
	"github.com/olekukonko/tablewriter"
	"github.com/openGemini/go-prompt"
	"github.com/openGemini/openGemini/app"
	"github.com/openGemini/openGemini/app/ts-cli/geminiql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"golang.org/x/term"
)

const (
	DEFAULT_FORMAT = "column"
	DEFAULT_HOST   = "localhost"
	DEFAULT_PORT   = 8086
)

var (
	FilePathCompletionSeparator = string([]byte{' ', os.PathSeparator})
)

type CommandLineConfig struct {
	Host       string
	Port       int
	UnixSocket string
	Username   string
	Password   string
	Database   string
	Type       string
	Ssl        bool
	IgnoreSsl  bool
	Precision  string

	// import cmd options
	Import bool
	Path   string
}

type HttpClient interface {
	Ping() (time.Duration, string, error)
	QueryContext(context.Context, client.Query) (*client.Response, error)
	Write(bp client.BatchPoints) (*client.Response, error)
	WriteLineProtocol(data, database, retentionPolicy, precision, writeConsistency string) (*client.Response, error)
	SetAuth(username, password string)
	SetPrecision(precision string)
}

type HttpClientCreator func(client.Config) (HttpClient, error)

func defaultHttpClientCreator(c client.Config) (HttpClient, error) {
	return client.NewClient(c)
}

type CommandLineFactory struct {
}

func (f CommandLineFactory) CreateCommandLine(config CommandLineConfig) (*CommandLine, error) {
	c := &CommandLine{
		cliConfig:     config,
		osSignals:     make(chan os.Signal, 1),
		parser:        geminiql.QLNewParser(),
		clientCreator: defaultHttpClientCreator,
	}

	addr := fmt.Sprintf("%s:%d/%s", config.Host, config.Port, "")
	url, err := client.ParseConnectionString(addr, config.Ssl)
	if err != nil {
		return nil, err
	}
	c.url = url
	c.ssl = config.Ssl

	c.config.UnixSocket = config.UnixSocket
	c.config.Username = config.Username
	c.config.Password = config.Password
	c.config.UnsafeSsl = config.IgnoreSsl
	c.config.Precision = config.Precision

	c.database = config.Database

	signal.Notify(c.osSignals, syscall.SIGINT, syscall.SIGTERM)

	return c, nil
}

type CommandLine struct {
	url           url.URL
	cliConfig     CommandLineConfig
	config        client.Config
	ssl           bool
	client        HttpClient
	clientCreator HttpClientCreator
	osSignals     chan os.Signal

	parser geminiql.QLParser

	retentionPolicy string
	database        string
	chunked         bool
	timer           bool
	chunkSize       int
	nodeID          int

	startTime time.Time

	serverVersion string
}

func (c *CommandLine) Connect(addr string) error {
	config := c.config

	addr = strings.TrimSpace(strings.Replace(strings.ToLower(addr), "connect", "", -1))
	if addr == "" {
		config.URL = c.url
	} else {
		url, err := client.ParseConnectionString(addr, c.ssl)
		if err != nil {
			return err
		}
		config.URL = url
	}

	config.UserAgent = "openGemini CLI/" + app.Version
	config.Proxy = http.ProxyFromEnvironment

	client, err := c.clientCreator(config)
	if err != nil {
		return fmt.Errorf("create http client failed: %s", err)
	}
	c.client = client

	_, v, err := c.client.Ping()
	if err != nil {
		return err
	}
	c.serverVersion = v

	c.config.URL = config.URL

	return nil
}

func (c *CommandLine) begin() {
	c.startTime = time.Now()
}

func (c *CommandLine) elapse() {
	d := time.Since(c.startTime)
	if c.timer {
		fmt.Printf("Elapsed: %v\n", d)
	}

}

func (c *CommandLine) tearDown(_ *prompt.Buffer) {
	if runtime.GOOS != "windows" {
		reset := exec.Command("stty", "-raw", "echo")
		reset.Stdin = os.Stdin
		_ = reset.Run()
	}
	os.Exit(0)
}

func (c *CommandLine) Execute(s string) error {
	var err error

	if s == "" {
		return nil
	} else if s == "quit" || s == "exit" {
		c.tearDown(nil)
	}

	ast := &geminiql.QLAst{}
	lexer := geminiql.QLNewLexer(geminiql.NewTokenizer(strings.NewReader(s)), ast)
	c.parser.Parse(lexer)

	c.startTime = time.Now()

	c.begin()
	defer c.elapse()

	if ast.Error == nil {
		err = c.executeOnLocal(ast.Stmt)
	} else {
		err = c.executeOnRemote(s)
	}

	return err
}

func (c *CommandLine) executor(s string) {
	if err := c.Execute(s); err != nil {
		fmt.Printf("ERR: %s\n", err)
	}
}

func (c *CommandLine) executeOnLocal(stmt geminiql.Statement) error {
	switch stmt := stmt.(type) {
	case *geminiql.InsertStatement:
		return c.executeInsert(stmt)
	case *geminiql.UseStatement:
		return c.executeUse(stmt)
	case *geminiql.ChunkedStatement:
		return c.executeChunked(stmt)
	case *geminiql.ChunkSizeStatement:
		return c.executeChunkSize(stmt)
	case *geminiql.AuthStatement:
		return c.executeAuth(stmt)
	case *geminiql.HelpStatement:
		return c.executeHelp(stmt)
	case *geminiql.PrecisionStatement:
		return c.executePrecision(stmt)
	case *geminiql.TimerStatement:
		return c.executeTimer(stmt)
	default:
		return fmt.Errorf("unsupport stmt %s", stmt)
	}
}

func (c *CommandLine) executeOnRemote(s string) error {
	return c.executeQuery(s)
}

func (c *CommandLine) executeInsert(stmt *geminiql.InsertStatement) error {
	bp := c.clientBatchPoints(stmt.DB,
		stmt.RP,
		stmt.LineProtocol)

	if _, err := c.client.Write(*bp); err != nil {
		return err
	}
	return nil
}

func parsePrecision(precision string) (string, error) {
	epoch := strings.ToLower(precision)

	switch epoch {
	case "":
		return "ns", nil
	case "h", "m", "s", "ms", "u", "ns", "rfc3339":
		return epoch, nil
	default:
		return "", fmt.Errorf("unknown precision %q. precision must be rfc3339, h, m, s, ms, u or ns", precision)
	}
}

func (c *CommandLine) executePrecision(stmt *geminiql.PrecisionStatement) error {
	var err error
	if stmt.Precision, err = parsePrecision(stmt.Precision); err != nil {
		return err
	}
	c.config.Precision = stmt.Precision
	// set precision for client
	c.client.SetPrecision(c.config.Precision)
	return nil
}

func (c *CommandLine) executeUse(stmt *geminiql.UseStatement) error {
	c.database = stmt.DB
	if stmt.RP == "" {
		c.retentionPolicy = ""
	} else {
		c.retentionPolicy = stmt.RP
	}
	return nil
}

func (c *CommandLine) executeQuery(query string) error {
	if c.retentionPolicy != "" {
		pq, err := influxql.NewParser(strings.NewReader(query)).ParseQuery()
		if err != nil {
			return err
		}
		for _, stmt := range pq.Statements {
			if selectStmt, ok := stmt.(*influxql.SelectStatement); ok {
				influxql.WalkFunc(selectStmt.Sources, func(n influxql.Node) {
					if m, ok := n.(*influxql.Measurement); ok {
						if m.Database == "" && c.database != "" {
							m.Database = c.database
						}
						if m.RetentionPolicy == "" && c.retentionPolicy != "" {
							m.RetentionPolicy = c.retentionPolicy
						}
					}
				})
			}
		}
		query = pq.String()
	}

	ctx := context.Background()
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)

	done := make(chan struct{})
	defer close(done)

	go func() {
		select {
		case <-done:
		case <-c.osSignals:
			cancel()
		}
	}()

	response, err := c.client.QueryContext(ctx, c.clientQuery(query))
	if err != nil {
		return err
	}

	if err := response.Error(); err != nil {
		return err
	}

	for _, result := range response.Results {
		for _, m := range result.Messages {
			fmt.Printf("%s: %s.\n", m.Level, m.Text)
		}
		c.prettyResult(result, os.Stdout)
	}

	return nil
}

func (c *CommandLine) executeChunked(stmt *geminiql.ChunkedStatement) error {
	// switch chunked model enable or disable
	c.chunked = !c.chunked
	displayFlag := "disabled"
	if c.chunked {
		displayFlag = "enabled"
	}
	fmt.Printf("Chunked responses %s\n", displayFlag)
	return nil
}

func (c *CommandLine) executeChunkSize(stmt *geminiql.ChunkSizeStatement) error {
	// The chunk size is only allowed between 0 and 9223372036854775807
	if stmt.Size < 0 {
		fmt.Printf("No allowed chunk size smaller than 0. Chunk size has been set to 0.")
		c.chunkSize = 0
	} else {
		c.chunkSize = int(stmt.Size)
	}
	return nil
}

func (c *CommandLine) executeAuth(stmt *geminiql.AuthStatement) error {
	fmt.Printf("username: ")
	fmt.Scanf("%s\n", &c.config.Username)
	fmt.Printf("password: ")
	password, _ := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Printf("\n")
	c.config.Password = string(password)
	c.client.SetAuth(c.config.Username, c.config.Password)
	return nil
}

func (c *CommandLine) executeHelp(stmt *geminiql.HelpStatement) error {
	fmt.Println(
		`
Usage:
	auth                    prompts for username and password
	chunked                 turns on chunked responses from server
	chunk_size <size>       sets the size of the chunked responses. Set to 0 to reset to the default chunked size
	use <db name>           sets current database
	precision <format>      specifies the format of the timestamp: rfc3339, h, m, s, ms, u or ns
	exit/quit/ctrl+d        quits the openGemini shell

	show databases          show database names
	show series             show series information
	show measurements       show measurement information
	show tag keys           show tag key information
	show field keys         show field key information

	A full list of openGemini commands can be found at:
	https://docs.opengemini.org
	`)
	return nil
}

func (c *CommandLine) executeTimer(stmt *geminiql.TimerStatement) error {
	// switch timer model enable or disable
	c.timer = !c.timer
	displayFlag := "disabled"
	if c.timer {
		displayFlag = "enabled"
	}
	fmt.Printf("Timer is %s\n", displayFlag)
	return nil
}

// printExplainAnalyze prints the explain analyze result.
//
// It takes a Row from QueryResponse and an io.Writer. It iterates through the
// row values and concatenates all strings into a single string. The string
// is then written to the io.Writer with a prefix of "EXPLAIN ANALYZE\n---------------\n".
func (c *CommandLine) printExplainAnalyze(result models.Row, w io.Writer) {
	var buff []string
	for _, value := range result.Values {
		for _, content := range value {
			s, ok := content.(string)
			if !ok {
				continue
			}
			buff = append(buff, s)
		}
	}
	fmt.Fprintf(w, "EXPLAIN ANALYZE\n---------------\n%s\n", strings.Join(buff, "\n"))
}

// prettyResult takes a Result and an io.Writer and prints the results in a
// pretty format. It prints the name and tags of each series, and then prints
// the columns and values of the series in a table format. Finally, it prints
// the number of columns and rows in the series.
func (c *CommandLine) prettyResult(result client.Result, w io.Writer) {
	for _, series := range result.Series {
		if len(series.Columns) == 0 {
			continue
		}
		columnName := series.Columns[0]
		if columnName == "EXPLAIN ANALYZE" {
			c.printExplainAnalyze(series, w)
			continue
		}

		var tags []string
		for k, v := range series.Tags {
			tags = append(tags, fmt.Sprintf("%s=%s", k, v))
			sort.Strings(tags)
		}

		if series.Name != "" {
			fmt.Fprintf(w, "name: %s\n", series.Name)
		}
		if len(tags) != 0 {
			fmt.Fprintf(w, "tags: %s\n", strings.Join(tags, ", "))
		}

		writer := tablewriter.NewWriter(w)
		c.prettyTable(series, writer)
		writer.Render()
		fmt.Fprintf(w, "%d columns, %d rows in set\n", len(series.Columns), len(series.Values))
	}
}

func (c *CommandLine) prettyTable(serie models.Row, w *tablewriter.Table) {
	w.SetAutoFormatHeaders(false)
	w.SetHeader(serie.Columns)
	for _, value := range serie.Values {
		tuple := make([]string, len(value))
		for i, val := range value {
			tuple[i] = fmt.Sprintf("%v", val)
		}
		w.Append(tuple)
	}
}

func (c *CommandLine) clientQuery(query string) client.Query {
	return client.Query{
		Command:         query,
		Database:        c.database,
		RetentionPolicy: c.retentionPolicy,
		Chunked:         c.chunked,
		ChunkSize:       c.chunkSize,
		NodeID:          c.nodeID,
	}
}

func (c *CommandLine) clientBatchPoints(db string, rp string, raw string) *client.BatchPoints {
	if db == "" {
		db = c.database
	}

	if rp == "" {
		rp = c.retentionPolicy
	}

	return &client.BatchPoints{
		Points: []client.Point{
			{Raw: raw},
		},
		Database:         db,
		RetentionPolicy:  rp,
		Precision:        c.config.Precision,
		WriteConsistency: c.config.WriteConsistency,
	}
}

func (c *CommandLine) Run() error {
	fmt.Printf("openGemini CLI %s (rev-%s)\n", app.Version, "revision")
	fmt.Println("Please use `quit`, `exit` or `Ctrl-D` to exit this program.")
	completer := NewCompleter()
	p := prompt.New(
		c.executor,
		completer.completer,
		prompt.OptionTitle("openGemini: interactive openGemini client"),
		prompt.OptionPrefix("> "),
		prompt.OptionPrefixTextColor(prompt.DefaultColor),
		prompt.OptionCompletionWordSeparator(FilePathCompletionSeparator),
		prompt.OptionAddASCIICodeBind(
			prompt.ASCIICodeBind{
				ASCIICode: []byte{0x1b, 0x62},
				Fn:        prompt.GoLeftWord,
			},
			prompt.ASCIICodeBind{
				ASCIICode: []byte{0x1b, 0x66},
				Fn:        prompt.GoRightWord,
			},
		),
		prompt.OptionAddKeyBind(
			prompt.KeyBind{
				Key: prompt.ShiftLeft,
				Fn:  prompt.GoLeftWord,
			},
			prompt.KeyBind{
				Key: prompt.ShiftRight,
				Fn:  prompt.GoRightWord,
			},
			prompt.KeyBind{
				Key: prompt.ControlC,
				Fn:  c.tearDown,
			},
		),
	)
	// Make sure key bind ControlD reset stty correctly
	defer c.tearDown(nil)
	p.Run()
	return nil
}
