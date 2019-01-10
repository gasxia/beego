package vanilla

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/kfchen81/beego"

	beego_context "github.com/kfchen81/beego/context"
	"github.com/kfchen81/beego/orm"
	"github.com/bitly/go-simplejson"
	"github.com/opentracing/opentracing-go"
	"encoding/json"
)

type RestResourceInterface interface {
	beego.ControllerInterface
	Resource() string
	EnableHTMLResource() bool
	IsForDevTest() bool
	DisableTx() bool
	GetParameters() map[string][]string
	GetBusinessContext() context.Context
	SetBeegoController(ctx *beego_context.Context, data map[interface{}]interface{})
}

/*RestResource 扩展beego.Controller, 作为rest中各个资源的基类
 */
type RestResource struct {
	beego.Controller

	Name2JSON      map[string]map[string]interface{}
	Name2JSONArray map[string][]interface{}
	Filters        map[string]interface{}
}

/*Resource 返回resource名
 */
func (r *RestResource) Resource() string {
	return ""
}

func (r *RestResource) SetBeegoController(ctx *beego_context.Context, data map[interface{}]interface{}) {
	r.Ctx = ctx
	r.Data = data
}

/*EnableHTMLResource 是否开启html资源
 */
func (r *RestResource) EnableHTMLResource() bool {
	return false
}

/*IsForDevTest 是否是开发时支持的资源
 */
func (r *RestResource) IsForDevTest() bool {
	return false
}

/*DisableTx 是否关闭事务支持
 */
func (r *RestResource) DisableTx() bool {
	return false
}

/*Parameters 获取需要检查的参数
 */
func (r *RestResource) GetParameters() map[string][]string {
	return nil
}

func (r *RestResource) GetBusinessContext() context.Context {
	data := r.Ctx.Input.GetData("bContext")
	if data == nil {
		return nil
	} else {
		bCtx := data.(context.Context)
		return bCtx
	}
}

//returnValidateParameterFailResponse 返回参数校验错误的response
func (r *RestResource) returnValidateParameterFailResponse(parameter string, paramType string, innerErrMsgs ...string) {
	innerErrMsg := ""
	if len(innerErrMsgs) > 0 {
		innerErrMsg = innerErrMsgs[0]
	}
	r.Data["json"] = &Response{
		500,
		nil,
		"rest:missing_argument",
		fmt.Sprintf("missing or invalid argument: %s(%s)", parameter, paramType),
		innerErrMsg,
	}
	r.ServeJSON()
}

/*Prepare 实现beego.Controller的Prepare函数
 */
func (r *RestResource) Prepare() {
	
	method := r.Ctx.Input.Method()
	r.Name2JSON = make(map[string]map[string]interface{})
	r.Name2JSONArray = make(map[string][]interface{})
	r.Filters = make(map[string]interface{})

	if app, ok := r.AppController.(RestResourceInterface); ok {
		method2parameters := app.GetParameters()
		if method2parameters != nil {
			if parameters, ok := method2parameters[method]; ok {
				actualParams := r.Input()
				for _, param := range parameters {
					colonPos := strings.Index(param, ":")
					paramType := "string"
					if colonPos != -1 {
						paramType = param[colonPos+1 : len(param)]
						param = param[0:colonPos]
					}

					canMissParam := false
					if param[0] == '?' {
						canMissParam = true
						param = param[1:]
					}
					if _, ok := actualParams[param]; !ok {
						if !canMissParam {
							r.returnValidateParameterFailResponse(param, paramType, "no paramter provided")
						} else {
							continue
						}
					}
					if paramType == "string" {
						//value := r.GetString(param)
					} else if paramType == "int" {
						_, err := r.GetInt64(param)
						if err != nil {
							r.returnValidateParameterFailResponse(param, paramType, err.Error())
						} else {
							//requestData[param] = value
						}
					} else if paramType == "float" {
						_, err := r.GetFloat(param)
						if err != nil {
							r.returnValidateParameterFailResponse(param, paramType, err.Error())
						} else {
							//requestData[param] = value
						}
					} else if paramType == "bool" {
						value := r.GetString(param)
						_, err := strconv.ParseBool(value)
						if err != nil {
							r.returnValidateParameterFailResponse(param, paramType, err.Error())
						} else {
							//requestData[param] = result
						}
					} else if paramType == "json" {
						value := r.GetString(param)
						if value == "" && canMissParam == true {
							goto set_orm
						}
						js, err := simplejson.NewJson([]byte(value))
						if err != nil {
							r.returnValidateParameterFailResponse(param, paramType, err.Error())
						} else {
							data, err := js.Map()
							if err != nil {
								r.returnValidateParameterFailResponse(param, paramType, err.Error())
							} else {
								if param == "filters" {
									r.Filters = data
								} else {
									r.Name2JSON[param] = data
								}
							}
						}
					} else if paramType == "json-array" {
						value := r.GetString(param)
						js, err := simplejson.NewJson([]byte(value))
						if err != nil {
							r.returnValidateParameterFailResponse(param, paramType, err.Error())
						} else {
							data, err := js.Array()
							if err != nil {
								r.returnValidateParameterFailResponse(param, paramType, err.Error())
							} else {
								r.Name2JSONArray[param] = data
							}
						}
					}
				}
set_orm:
				for key, _ := range actualParams {
					if strings.HasPrefix(key, "__f") {
						r.Filters[key] = actualParams.Get(key)
					}
				}

				bCtx := r.GetBusinessContext()
				o := GetOrmFromContext(bCtx)
				r.Ctx.Input.Data()["sessionOrm"] = o
				if !r.Ctx.ResponseWriter.Started {
					if o != nil {
						if app, ok := r.AppController.(RestResourceInterface); ok {
							if !app.DisableTx() {
								o.Begin()
								beego.Debug("[ORM] start transaction")
							}
						}
					}
				} else {
				}
			}
		}
	}
}

func (r *RestResource) Finish() {
	bCtx := r.GetBusinessContext()
	if bCtx != nil {
		o := bCtx.Value("orm")
		if o != nil {
			if app, ok := r.AppController.(RestResourceInterface); ok {
				if !app.DisableTx() {
					o.(orm.Ormer).Commit()
					beego.Debug("[ORM] commit transaction")
				}
			}
		}

		span := opentracing.SpanFromContext(bCtx)
		if span != nil {
			beego.Debug("[Tracing] finish span in Controller.Finish")
			span.(opentracing.Span).Finish()
		}
	}
}

//GetJSONArray 与key对应的返回json array数据
func (r *RestResource) GetJSONArray(key string) []interface{} {
	if data, ok := r.Name2JSONArray[key]; ok {
		return data
	} else {
		return nil
	}
}

func (r *RestResource) GetIntArray(key string) []int {
	values := make([]int, 0)
	if datas, ok := r.Name2JSONArray[key]; ok {
		for _, data := range datas {
			intValue, _ := strconv.Atoi(data.(json.Number).String())
			values = append(values, intValue)
		}
		return values
	} else {
		return nil
	}
}

func (r *RestResource) GetStringArray(key string) []string {
	values := make([]string, 0)
	if datas, ok := r.Name2JSONArray[key]; ok {
		for _, data := range datas {
			values = append(values, data.(string))
		}
		return values
	} else {
		return nil
	}
}

//GetJSONArray 与key对应的返回json map数据
func (r *RestResource) GetJSON(key string) map[string]interface{} {
	if data, ok := r.Name2JSON[key]; ok {
		return data
	} else {
		return nil
	}
}

func (r *RestResource) GetFillOptions(key string) FillOption {
	fillOption := FillOption{}
	if data, ok := r.Name2JSON[key]; ok {
		for key, _ := range data {
			fillOption[key] = true
		}
	} else {
		return nil
	}
	
	return fillOption
}

// 返回filters参数与__f标准查询的map数据
func (r *RestResource) GetFilters() map[string]interface{} {
	return r.Filters
}

/*ReturnJSON 返回json response*/
func (r *RestResource) ReturnJSON(response *Response) {
	r.Data["json"] = response
	r.ServeJSON()
}