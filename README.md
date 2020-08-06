SDK使用方法

1.sn列表文件
放置在
%APPRoot%/conf/snlist.properties

2.配置文件
放置在
%APPRoot%/conf/ytfs.properties

3.初始化SDK
启动服务后执行一次
api.StartApi()

4.创建用户端实例
user：用户名
pkey：用户存储私钥
c, err := api.NewClient(user, pkey)
err:当SN通讯或其他服务故障造成实例化失败，caller可间隔n秒重试
c：用户端实例

c.UserId:注册成功后sn返回的用户ID
c.SuperNode:用户所属的SN节点信息

SDK支持实例华多个用户端，最大2000

5.上传文件
upload:=c.NewUploadObject()
//上传本地文件
path:本地文件路径
hash,err:=upload.UploadFile(path)
hash:上传成功后的文件sha256摘要
err:上传失败信息{Code：错误码，Msg：错误描述}

//上传[]byte
hash,err:=upload.UploadBytes(bytes)
返回同上

//上传进度，百分比（ii%）
ii:=upload.GetProgress()

6.下载文件
hash:要下载的文件sha256摘要
download,err:=c.NewDownloadObject(hash)
download：下载实例
err:下载实例化出错，可能SN通讯或其他服务故障

获取输出流,可按字节范围下载
reader:=download.Load()
reader:=download.LoadRange(startpos,endpos)
reader：返回标准库io.Reader类型实例

下载文件到磁盘指定位置
err:=download.SaveToFile(path)





