package handle

type HandlerInitor func() MessageEvent

var ID_HANDLER_MAP = make(map[uint16]HandlerInitor)

func init() {














}