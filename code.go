package code

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

const (
	DriverPostgres   = "postgres"
	DriverMysql      = "mysql"
	DriverMssql      = "mssql"
	DriverOracle     = "oracle"
	DriverSqlite3    = "sqlite3"
	DriverNotSupport = "no support"
)

type CodeModel struct {
	Id       string `mapstructure:"id" json:"id,omitempty" gorm:"column:id" bson:"id,omitempty" dynamodbav:"id,omitempty" firestore:"id,omitempty"`
	Code     string `mapstructure:"code" json:"code,omitempty" gorm:"column:code" bson:"code,omitempty" dynamodbav:"code,omitempty" firestore:"code,omitempty"`
	Value    string `mapstructure:"value" json:"value,omitempty" gorm:"column:value" bson:"value,omitempty" dynamodbav:"value,omitempty" firestore:"value,omitempty"`
	Name     string `mapstructure:"name" json:"name,omitempty" gorm:"column:name" bson:"name,omitempty" dynamodbav:"name,omitempty" firestore:"name,omitempty"`
	Text     string `mapstructure:"text" json:"text,omitempty" gorm:"column:text" bson:"text,omitempty" dynamodbav:"text,omitempty" firestore:"text,omitempty"`
	Sequence int32  `mapstructure:"sequence" json:"sequence,omitempty" gorm:"column:sequence" bson:"sequence,omitempty" dynamodbav:"sequence,omitempty" firestore:"sequence,omitempty"`
}
type CodeConfig struct {
	Master   string      `mapstructure:"master" json:"master,omitempty" gorm:"column:master" bson:"master,omitempty" dynamodbav:"master,omitempty" firestore:"master,omitempty"`
	Id       string      `mapstructure:"id" json:"id,omitempty" gorm:"column:id" bson:"id,omitempty" dynamodbav:"id,omitempty" firestore:"id,omitempty"`
	Code     string      `mapstructure:"code" json:"code,omitempty" gorm:"column:code" bson:"code,omitempty" dynamodbav:"code,omitempty" firestore:"code,omitempty"`
	Text     string      `mapstructure:"text" json:"text,omitempty" gorm:"column:text" bson:"text,omitempty" dynamodbav:"text,omitempty" firestore:"text,omitempty"`
	Name     string      `mapstructure:"name" json:"name,omitempty" gorm:"column:name" bson:"name,omitempty" dynamodbav:"name,omitempty" firestore:"name,omitempty"`
	Value    string      `mapstructure:"value" json:"value,omitempty" gorm:"column:value" bson:"value,omitempty" dynamodbav:"value,omitempty" firestore:"value,omitempty"`
	Sequence string      `mapstructure:"sequence" json:"sequence,omitempty" gorm:"column:sequence" bson:"sequence,omitempty" dynamodbav:"sequence,omitempty" firestore:"sequence,omitempty"`
	Status   string      `mapstructure:"status" json:"status,omitempty" gorm:"column:status" bson:"status,omitempty" dynamodbav:"status,omitempty" firestore:"status,omitempty"`
	Active   interface{} `mapstructure:"active" json:"active,omitempty" gorm:"column:active" bson:"active,omitempty" dynamodbav:"active,omitempty" firestore:"active,omitempty"`
}
type CodeLoader interface {
	Load(ctx context.Context, master string) ([]CodeModel, error)
}
type SqlCodeLoader struct {
	DB     *sql.DB
	Table  string
	Config CodeConfig
	Build  func(i int) string
	Map    func(col string) string
}
type DynamicSqlCodeLoader struct {
	DB             *sql.DB
	Query          string
	ParameterCount int
	Map            func(string) string
}

func NewDefaultDynamicSqlCodeLoader(db *sql.DB, query string, options ...int) *DynamicSqlCodeLoader {
	var parameterCount int
	if len(options) >= 1 && options[0] > 0 {
		parameterCount = options[0]
	} else {
		parameterCount = 0
	}
	return NewDynamicSqlCodeLoader(db, query, parameterCount, true)
}
func NewDynamicSqlCodeLoader(db *sql.DB, query string, parameterCount int, options ...bool) *DynamicSqlCodeLoader {
	driver := getDriver(db)
	var mp func(string) string
	if driver == DriverOracle {
		mp = strings.ToUpper
	}
	if parameterCount <= 0 {
		parameterCount = 1
	}
	var handleDriver bool
	if len(options) >= 1 {
		handleDriver = options[0]
	} else {
		handleDriver = true
	}
	if handleDriver {
		if driver == DriverOracle || driver == DriverPostgres || driver == DriverMssql {
			var x string
			if driver == DriverOracle {
				x = ":val"
			} else if driver == DriverPostgres {
				x = "$"
			} else if driver == DriverMssql {
				x = "@p"
			}
			for i := 0; i < parameterCount; i++ {
				count := i + 1
				query = strings.Replace(query, "?", x+strconv.Itoa(count), 1)
			}
		}
	}
	return &DynamicSqlCodeLoader{DB: db, Query: query, ParameterCount: parameterCount, Map: mp}
}
func (l DynamicSqlCodeLoader) Load(ctx context.Context, master string) ([]CodeModel, error) {
	models := make([]CodeModel, 0)
	params := make([]interface{}, 0)
	params = append(params, master)
	if l.ParameterCount > 1 {
		for i := 2; i <= l.ParameterCount; i++ {
			params = append(params, master)
		}
	}
	rows, er1 := l.DB.QueryContext(ctx, l.Query, params...)
	if er1 != nil {
		return models, er1
	}
	defer rows.Close()
	columns, er2 := rows.Columns()
	if er2 != nil {
		return models, er2
	}
	// get list indexes column
	modelTypes := reflect.TypeOf(models).Elem()
	modelType := reflect.TypeOf(CodeModel{})
	indexes, er3 := getColumnIndexes(modelType, columns, l.Map)
	if er3 != nil {
		return models, er3
	}
	tb, er4 := scanType(rows, modelTypes, indexes)
	if er4 != nil {
		return models, er4
	}
	for _, v := range tb {
		if c, ok := v.(*CodeModel); ok {
			models = append(models, *c)
		}
	}
	return models, nil
}
func NewSqlCodeLoader(db *sql.DB, table string, config CodeConfig, options...func(i int) string) *SqlCodeLoader {
	var build func(i int) string
	if len(options) > 0 && options[0] != nil {
		build = options[0]
	} else {
		build = getBuild(db)
	}
	driver := getDriver(db)
	var mp func(string) string
	if driver == DriverOracle {
		mp = strings.ToUpper
	}
	return &SqlCodeLoader{DB: db, Table: table, Config: config, Build: build, Map: mp}
}
func (l SqlCodeLoader) Load(ctx context.Context, master string) ([]CodeModel, error) {
	models := make([]CodeModel, 0)
	s := make([]string, 0)
	values := make([]interface{}, 0)
	sql2 := ""

	c := l.Config
	if len(c.Id) > 0 {
		sf := fmt.Sprintf("%s as id", c.Id)
		s = append(s, sf)
	}
	if len(c.Code) > 0 {
		sf := fmt.Sprintf("%s as code", c.Code)
		s = append(s, sf)
	}
	if len(c.Name) > 0 {
		sf := fmt.Sprintf("%s as name", c.Name)
		s = append(s, sf)
	}
	if len(c.Value) > 0 {
		sf := fmt.Sprintf("%s as value", c.Value)
		s = append(s, sf)
	}
	if len(c.Text) > 0 {
		sf := fmt.Sprintf("%s as text", c.Text)
		s = append(s, sf)
	}
	osequence := ""
	if len(c.Sequence) > 0 {
		osequence = fmt.Sprintf("order by %s", c.Sequence)
	}
	p1 := ""
	i := 1
	if len(c.Master) > 0 {
		p1 = fmt.Sprintf("%s = %s", c.Master, l.Build(i))
		i = i + 1
		values = append(values, master)
	}
	cols := strings.Join(s, ",")
	if len(c.Status) > 0 && c.Active != nil {
		p2 := fmt.Sprintf("%s = %s", c.Status, l.Build(i))
		values = append(values, c.Active)
		if cols == "" {
			cols = "*"
		}
		if len(p1) > 0 {
			sql2 = fmt.Sprintf("select %s from %s where %s and %s %s", cols, l.Table, p1, p2, osequence)
		} else {
			sql2 = fmt.Sprintf("select %s from %s where %s %s", cols, l.Table, p2, osequence)
		}
	} else {
		if cols == "" {
			cols = "*"
		}
		if len(p1) > 0 {
			sql2 = fmt.Sprintf("select %s from %s where %s %s", cols, l.Table, p1, osequence)
		} else {
			sql2 = fmt.Sprintf("select %s from %s %s", cols, l.Table, osequence)
		}
	}
	if len(sql2) > 0 {
		rows, err1 := l.DB.QueryContext(ctx, sql2, values...)
		if err1 != nil {
			return nil, err1
		}
		defer rows.Close()
		columns, er1 := rows.Columns()
		if er1 != nil {
			return nil, er1
		}
		// get list indexes column
		modelTypes := reflect.TypeOf(models).Elem()
		modelType := reflect.TypeOf(CodeModel{})
		indexes, er2 := getColumnIndexes(modelType, columns, l.Map)
		if er2 != nil {
			return nil, er2
		}
		tb, er3 := scanType(rows, modelTypes, indexes)
		if er3 != nil {
			return nil, er3
		}
		for _, v := range tb {
			if c, ok := v.(*CodeModel); ok {
				models = append(models, *c)
			}
		}
	}
	return models, nil
}

// StructScan : transfer struct to slice for scan
func structScan(s interface{}, indexColumns []int) (r []interface{}) {
	if s != nil {
		maps := reflect.Indirect(reflect.ValueOf(s))
		for _, index := range indexColumns {
			r = append(r, maps.Field(index).Addr().Interface())
		}
	}
	return
}
func getColumnIndexes(modelType reflect.Type, columnsName []string, build func(string) string) (indexes []int, err error) {
	if modelType.Kind() != reflect.Struct {
		return nil, errors.New("bad type")
	}
	for i := 0; i < modelType.NumField(); i++ {
		field := modelType.Field(i)
		ormTag := field.Tag.Get("gorm")
		column, ok := findTag(ormTag, "column")
		if build != nil {
			column = build(column)
		}
		if ok {
			if contains(columnsName, column) {
				indexes = append(indexes, i)
			}
		}
	}
	return
}
func findTag(tag string, key string) (string, bool) {
	if has := strings.Contains(tag, key); has {
		str1 := strings.Split(tag, ";")
		num := len(str1)
		for i := 0; i < num; i++ {
			str2 := strings.Split(str1[i], ":")
			for j := 0; j < len(str2); j++ {
				if str2[j] == key {
					return str2[j+1], true
				}
			}
		}
	}
	return "", false
}

func contains(array []string, v string) bool {
	for _, s := range array {
		if s == v {
			return true
		}
	}
	return false
}

func scanType(rows *sql.Rows, modelTypes reflect.Type, indexes []int) (t []interface{}, err error) {
	for rows.Next() {
		initArray := reflect.New(modelTypes).Interface()
		if err = rows.Scan(structScan(initArray, indexes)...); err == nil {
			t = append(t, initArray)
		}
	}
	return
}

func buildParam(i int) string {
	return "?"
}
func buildOracleParam(i int) string {
	return ":val" + strconv.Itoa(i)
}
func buildMsSqlParam(i int) string {
	return "@p" + strconv.Itoa(i)
}
func buildDollarParam(i int) string {
	return "$" + strconv.Itoa(i)
}
func getBuild(db *sql.DB) func(i int) string {
	driver := reflect.TypeOf(db.Driver()).String()
	switch driver {
	case "*pq.Driver":
		return buildDollarParam
	case "*godror.drv":
		return buildOracleParam
	case "*mysql.MySQLDriver":
		return buildMsSqlParam
	default:
		return buildParam
	}
}
func getDriver(db *sql.DB) string {
	if db == nil {
		return DriverNotSupport
	}
	driver := reflect.TypeOf(db.Driver()).String()
	switch driver {
	case "*pq.Driver":
		return DriverPostgres
	case "*godror.drv":
		return DriverOracle
	case "*mysql.MySQLDriver":
		return DriverMysql
	case "*mssql.Driver":
		return DriverMssql
	case "*sqlite3.SQLiteDriver":
		return DriverSqlite3
	default:
		return DriverNotSupport
	}
}
