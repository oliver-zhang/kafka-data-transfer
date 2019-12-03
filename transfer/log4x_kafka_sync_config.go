package transfer

import (
	"errors"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
)

type Log4xKafkaSyncConfig struct {
	Kafka Kafka
}

type Kafka struct {
	Origin             string
	Target             string
	KafkaExcludeTopics string
	KafkaIncludeTopics string
}

func LoadLog4xKafkaSyncConfig(file *os.File) (config Log4xKafkaSyncConfig, err error) {
	var log4xKafkaSyncConfig Log4xKafkaSyncConfig
	yamlFile, err := ioutil.ReadFile(file.Name())
	if err != nil {
		return log4xKafkaSyncConfig, errors.New("读取配置文件错误" + err.Error())
	}
	err = yaml.Unmarshal(yamlFile, &log4xKafkaSyncConfig)
	if err != nil {
		return log4xKafkaSyncConfig, errors.New("读取配置文件错误" + err.Error())
	}
	return log4xKafkaSyncConfig, nil
}
