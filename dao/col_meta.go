package dao

import (
	"context"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const SUPER_NODE = "supernodes"
const SUPER_NODE_INDEX = "supernodes_id"

const USER_TABLE_NAME = "users"
const USER_INDEX_NAME = "username"
const USER_REL_INDEX_NAME = "relationship"

const BLOCK_TABLE_NAME = "blocks"
const BLOCK_INDEX_VHP_VHB = "VHP_VHB"
const BLOCK_DAT_TABLE_NAME = "blocks_data"
const BLOCK_BK_TABLE_NAME = "blocks_bak"
const BLOCK_CNT_TABLE_NAME = "block_count"

const SHARD_TABLE_NAME = "shards"
const SHARD_CNT_TABLE_NAME = "shard_count"
const SHARD_RBD_TABLE_NAME = "shards_rebuild"

type MetaBaseSource struct {
	db           *mongo.Database
	user_c       *mongo.Collection
	block_c      *mongo.Collection
	block_d_c    *mongo.Collection
	block_cnt_c  *mongo.Collection
	block_bk_c   *mongo.Collection
	shard_c      *mongo.Collection
	shard_cnt_c  *mongo.Collection
	shard_rbd_c  *mongo.Collection
	supernodes_c *mongo.Collection
}

var metaBaseSource *MetaBaseSource = nil

func NewBaseSource() *MetaBaseSource {
	return metaBaseSource
}

func (source *MetaBaseSource) initMetaDB() {
	source.db = session.Database(DATABASENAME)
	source.user_c = source.db.Collection(USER_TABLE_NAME)
	index1 := mongo.IndexModel{
		Keys:    bson.M{"username": 1},
		Options: options.Index().SetUnique(true).SetName(USER_INDEX_NAME),
	}
	source.user_c.Indexes().CreateOne(context.Background(), index1)
	index2 := mongo.IndexModel{
		Keys:    bson.M{"relationship": 1},
		Options: options.Index().SetUnique(false).SetName(USER_REL_INDEX_NAME),
	}
	source.user_c.Indexes().CreateOne(context.Background(), index2)

	source.block_c = source.db.Collection(BLOCK_TABLE_NAME)
	index3 := mongo.IndexModel{
		Keys:    bson.M{"VHP": 1, "VHB": 1},
		Options: options.Index().SetUnique(true).SetName(BLOCK_INDEX_VHP_VHB),
	}
	source.block_c.Indexes().CreateOne(context.Background(), index3)
	source.block_d_c = source.db.Collection(BLOCK_DAT_TABLE_NAME)
	source.block_cnt_c = source.db.Collection(BLOCK_CNT_TABLE_NAME)
	source.block_bk_c = source.db.Collection(BLOCK_BK_TABLE_NAME)
	source.shard_c = source.db.Collection(SHARD_TABLE_NAME)
	source.shard_cnt_c = source.db.Collection(SHARD_CNT_TABLE_NAME)
	source.shard_rbd_c = source.db.Collection(SHARD_RBD_TABLE_NAME)
	source.supernodes_c = source.db.Collection(SUPER_NODE)
	index4 := mongo.IndexModel{
		Keys:    bson.M{"snid": 1},
		Options: options.Index().SetUnique(true).SetName(SUPER_NODE_INDEX),
	}
	source.supernodes_c.Indexes().CreateOne(context.Background(), index4)
	logrus.Infof("[InitMongo]Create metabase tables Success.\n")
}

func (source *MetaBaseSource) GetDB() *mongo.Database {
	return source.db
}

func (source *MetaBaseSource) GetColl(name string) *mongo.Collection {
	if name == USER_TABLE_NAME {
		return source.user_c
	} else if name == BLOCK_TABLE_NAME {
		return source.block_c
	} else if name == BLOCK_DAT_TABLE_NAME {
		return source.block_d_c
	} else if name == BLOCK_BK_TABLE_NAME {
		return source.block_bk_c
	} else if name == BLOCK_CNT_TABLE_NAME {
		return source.block_cnt_c
	} else if name == SHARD_TABLE_NAME {
		return source.shard_c
	} else if name == SHARD_CNT_TABLE_NAME {
		return source.shard_cnt_c
	} else if name == SHARD_RBD_TABLE_NAME {
		return source.shard_rbd_c
	} else if name == SUPER_NODE {
		return source.supernodes_c
	}
	return nil
}

func (source *MetaBaseSource) GetUserColl() *mongo.Collection {
	return source.user_c
}

func (source *MetaBaseSource) GetBlockColl() *mongo.Collection {
	return source.block_c
}

func (source *MetaBaseSource) GetBlockDataColl() *mongo.Collection {
	return source.block_d_c
}

func (source *MetaBaseSource) GetBlockBakColl() *mongo.Collection {
	return source.block_bk_c
}

func (source *MetaBaseSource) GetBlockCountColl() *mongo.Collection {
	return source.block_cnt_c
}

func (source *MetaBaseSource) GetShardColl() *mongo.Collection {
	return source.shard_c
}

func (source *MetaBaseSource) GetShardCountColl() *mongo.Collection {
	return source.shard_cnt_c
}

func (source *MetaBaseSource) GetShardRebuildColl() *mongo.Collection {
	return source.shard_rbd_c
}

func (source *MetaBaseSource) GetSuperNodesColl() *mongo.Collection {
	return source.supernodes_c
}
