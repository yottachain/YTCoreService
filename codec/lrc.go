package codec

type LRC struct {
	data []byte
	VHF  []byte
}

func LRC_Initial(globalRecoveryCount, maxHandles uint16) uint16 {

	return 0
}
