package vanilla

import (
	"github.com/kfchen81/beego/context"
	
	"github.com/kfchen81/beego"
	"github.com/kfchen81/beego/orm"
	"github.com/kfchen81/beego/logs"
	"github.com/opentracing/opentracing-go"
	"fmt"
	"bytes"
	"runtime"
	"strings"
)

func RecoverPanic(ctx *context.Context) {
	if err := recover(); err != nil {
		//rollback tx
		o := ctx.Input.Data()["sessionOrm"]
		if o != nil {
			o.(orm.Ormer).Rollback()
			beego.Warn("[ORM] rollback transaction")
		}
		
		//finish span
		span := ctx.Input.GetData("span")
		if span != nil {
			beego.Info("[Tracing] finish span in recoverPanic")
			span.(opentracing.Span).Finish()
		}
		
		if err == beego.ErrAbort {
			return
		}
		if !beego.BConfig.RecoverPanic {
			panic(err)
		}
		
		errMsg := ""
		if be, ok := err.(*BusinessError); ok {
			errMsg = fmt.Sprintf("%s:%s", be.ErrCode, be.ErrMsg)
			
		} else {
			errMsg = fmt.Sprintf("%s", err)
		}
		
		//log error info
		var buffer bytes.Buffer
		buffer.WriteString(fmt.Sprintf("[Unprocessed_Exception] %s\n", errMsg))
		buffer.WriteString(fmt.Sprintf("Request URL: %s\n", ctx.Input.URL()))
		for i := 1; ; i++ {
			_, file, line, ok := runtime.Caller(i)
			if !ok {
				break
			}
			buffer.WriteString(fmt.Sprintf("%s:%d\n", file, line))
		}
		if beego.BConfig.RunMode == "dev" {
			logs.Critical(buffer.String())
		} else {
			logs.Critical(strings.Replace(buffer.String(), "\n", ";  ", -1))
		}
		
		//return error response
		var resp Map
		if be, ok := err.(*BusinessError); ok {
			resp = Map{
				"code":        500,
				"data":        nil,
				"errCode":     be.ErrCode,
				"errMsg":      be.ErrMsg,
				"innerErrMsg": "",
			}
		} else {
			endpoint := ctx.Request.RequestURI
			pos := strings.Index(endpoint, "?")
			if pos != -1 {
				endpoint = endpoint[:pos]
			}
			resp = Map{
				"code":        531,
				"data":        Map{
					"endpoint": endpoint,
				},
				"errCode":     "system:exception",
				"errMsg":      fmt.Sprintf("%s", err),
				"innerErrMsg": "",
			}
		}
		ctx.Output.JSON(resp, true, true)
	}
}