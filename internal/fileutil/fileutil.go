package fileutil

import "os"

func OpenFile(fileName string)(*os.File,error){
	fp,err :=  os.Open(fileName)
	if err != nil{
		return nil, err
	}
	if _,err := fp.Stat();err != nil{
		if fp != nil{
			fp.Close()
		}
		return nil, err
	}
	return fp, nil
}
