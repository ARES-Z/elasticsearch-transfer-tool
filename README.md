# elasticsearch-transfer-tool
elasticsearch 数据迁移工具
* 支持index setting迁移
* 支持index mapping迁移
* 支持index 数据迁移

```
usage: transfer
 -data                      迁移数据
 -mapping                   迁移mappings
 -setting                   迁移settings
 -sh,--source-host <arg>    数据源
 -si,--source-index <arg>   要迁移的index
 -st,--source-type <arg>    要迁移的type
 -th,--target-host <arg>    数据源
 -ti,--target-index <arg>   迁移后的index
 -tt,--target-type <arg>    迁移后的type
 ```
