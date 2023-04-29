package main

import (
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	algorithmFlag      string
	numberEpisodesFlag int64
	loggers            []string
)

var RootCmd = &cobra.Command{
	Use:   "spice",
	Short: "Spice.xyz CLI",
}

// Execute adds all child commands to the root command.
func Execute() {
	cobra.OnInitialize(initConfig)

	if err := RootCmd.Execute(); err != nil {
		RootCmd.Println(err)
		os.Exit(-1)
	}
}

func initConfig() {
	viper.SetEnvPrefix("spice")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()
}
