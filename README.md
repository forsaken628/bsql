# bsql

[![Build Status](https://api.travis-ci.com/forsaken628/bsql.svg?branch=master)](https://www.travis-ci.org/forsaken628/bsql)
[![Hex.pm](https://img.shields.io/hexpm/l/plug.svg)](https://github.com/github.com/forsaken628/blob/master/LICENSE)
[![GoDoc](https://godoc.org/github.com/forsaken628/bsql?status.svg)](https://godoc.org/github.com/forsaken628/bsql)

bsql是一个sql辅助库，用于替代繁琐易错的纯手工拼接。

## 前言

使用orm通常会遇到一种非常尴尬的情况：

需要的功能没有，不需要的功能一堆，即使有也需要花很多时间去学习如何使用，踩一堆坑；
写的时候先构思sql，再翻译成orm的api，调试的时候总想看看实际执行的sql到底是什么；
复杂sql的优化终究还是要靠手写。

但是纯手工拼接即易错，繁琐，又容易被注入。

之前一直使用[github.com/didi/gendry](https://github.com/didi/gendry)，但是gendry最大的缺点在于过于依赖map，一方面除非改源码没办法扩展，另一方面实现复杂，导致效率不高。

而bsql是一个用go的方式来解决这个问题的库。bsql仅用一层非常薄的封装将sql结构化，基于一个统一的抽象，`Builder`接口，一方面最大限度的保持灵活性和可扩展性，另一方面保留sql原有的结构，所见即所得。

## 快速入门

#### example

```go
package main

import (
	"database/sql"
	"fmt"

	"github.com/forsaken628/bsql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

func main() {
	db, err := sqlx.Open("mysql", "xxxxxxxxxxx")
	if nil != err {
		panic(err)
	}
	db = db.Unsafe()

	q, a := bsql.Select{
		Table:  bsql.Raw("tableName"),
		Fields: []string{"name", "count(price) as total", "age"},
		Where: bsql.SecAND{
			bsql.Raw("country = ?", "China"),
			bsql.Raw("role = ?", "driver"),
			bsql.Raw("age > ?", "45"),
		},
		GroupBy: "name",
		Having: bsql.SecAND{
			bsql.Raw("total > ?", 1000),
			bsql.Raw("total <= ?", 50000),
		},
		OrderBy: []string{"age desc"},
	}.Build()

	//q: SELECT name,count(price) as total,age FROM tableName WHERE (country = ? AND role = ? AND age > ?) GROUP BY name HAVING (total > ? AND total <= ?) ORDER BY age desc
	//a: []interface{}{"China","driver",45,1000,50000}

	dest := struct {
		Name  string `db:"name"`
		Total int    `db:"total"`
		Age   int    `db:"age"`
	}{}

	err = db.Select(&dest, q, a...)
	if nil != err {
		panic(err)
	}

	fmt.Println(dest)
}
```

## API

#### `Select`

```go
bsql.Select{
	Table:  bsql.Raw("tableName"),
	Fields: []string{"name", "count(price) as total", "age"},
	Where: bsql.SecAND{
		bsql.Raw("country = ?", "China"),
		bsql.Raw("role = ?", "driver"),
		bsql.Raw("age > ?", "45"),
	},
	GroupBy: "name",
	Having: bsql.SecAND{
		bsql.Raw("total > ?", 1000),
		bsql.Raw("total <= ?", 50000),
	},
	OrderBy: []string{"age desc"},
}
```

#### `Update`

```go
bsql.Update{
	Table: bsql.Raw("tableName"),
	Set: bsql.MakeSet(map[string]interface{}{
		"district": 50,
		"score":    "010",
	}),
	Where: bsql.SecAND{
		bsql.Raw("foo = ?", "bar"),
		bsql.Raw("age >= ?", 23),
		bsql.MakeIn("sex", []interface{}{"male", "female"}),
	},
}
```

#### `Insert`

```go
b, err := bsql.MakeValues(
	[]string{"age", "foo"},
	[][]interface{}{
		{23, "bar"},
	})
if err != nil {
	panic(err)
}

bsql.Insert{
	Table: bsql.Raw("tableName"),
	Value: b,
}
```

#### `Delete`

```go
bsql.Delete{
	Table: bsql.Raw("tableName"),
	Where: bsql.SecAND{
		bsql.MakeIn("hobby", []interface{}{"soccer", "basketball", "tenis"}),
		bsql.MakeIn("sex", []interface{}{"male", "female"}),
		bsql.Raw("age >= ?", 21),
	},
}
```

### 安全
如果您使用`Prepare && stmt.SomeMethods`，那么您无需担心安全问题。
Prepare使用mysql的二进制协议，会将请求语句与参数分开处理，使sql注入完全无效。
因此构建器不会转义它收到的字符串值，这是没有意义。

另外请勿将其作为驱动变量之一的interpolateParams设置为true，这样会将参数插入语句，降级到文本协议。

### 感谢

[github.com/didi/gendry](https://github.com/didi/gendry)为bsql的编写提供灵感和测试用例。
