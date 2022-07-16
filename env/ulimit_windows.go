package env

import "golang.org/x/sys/windows/registry"

func ULimit() {

}

func GetWinVersion() (string, error) {
	k, err := registry.OpenKey(registry.LOCAL_MACHINE, `SOFTWARE\Microsoft\Windows NT\CurrentVersion`, registry.QUERY_VALUE)
	if err != nil {
		return "", err
	}
	defer k.Close()
	cv, _, err := k.GetStringValue("CurrentVersion")
	if err != nil {
		return "", err
	}
	return cv, nil
}
