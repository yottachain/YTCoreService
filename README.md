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
c.AccessorKey:用户端实例唯一标识,用户公钥
SDK支持实例华多个用户端，最大2000

通过AccessorKey获取用户端实例
c:=api.GetClient(AccessorKey)

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
//上传完毕后的文件版本号
vnu:=upload.VNU

文件上传成功后，写入用户元数据
objectAccessor:=c.NewObjectAccessor()
VNU:上面上传完毕后的返回的文件版本号
meta：文件属性，自定义格式
objectAccessor.CreateObject(bucketname, filename, VNU , meta)

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

指定文件名及版本号下载文件
version:不指定版本号，下载最新版本
download,err:=c.NewDownloadBytes(bucketName, filename , version)

7.用户元数据接口
创建bucket
bucketAccessor:=c.NewBucketAccessor()
meta:Bucket属性，自定义
err:=bucketAccessor.CreateBucket(bucketName,meta)

更新Bucket属性
err:=bucketAccessor.UpdateBucket(bucketName,meta)

获取Bucket属性
meta,err:=bucketAccessor.GetBucket(bucketName)

删除Bucket
err:=bucketAccessor.DeleteBucket(bucketName)

返回所有bucket
names,err:=bucketAccessor.ListBucket()

LS文件列表
objectAccessor:=c.NewObjectAccessor()
fileName：开始文件名
prefix:文件前缀
nVerid:开始文件版本号
wverion:false返回最新版本，true返回所有版本，这时nVerid的输入值生效
limit：返回条数
ls,err:=objectAccessor.ListObject(buck,fileName,prefix,nVerid,wverion,limit uint32)

文件拷贝，将文件元数据拷贝到另一个bucket，并指定新文件名
err:=objectAccessor.ListObject(srcbuck, srckey, destbuck, destkey)

删除文件
Verid:如果nil，删除所有版本，否则删除指定版本
err:=objectAccessor.DeleteObject(buck, fileName, Verid) 

获取文件元数据
item,err:=objectAccessor.GetObject(buck, fileName)
返回
item.FileId 文件ID，如果nil表示文件不存在








