package manipcli

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.acuvity.ai/elemental"
	"go.acuvity.ai/manipulate"
	"go.acuvity.ai/manipulate/maniphttp"
)

// cliConfig hods the generate configuration.
type cliConfig struct {
	ignoredIdentities map[string]struct{}
	argumentsPrefix   string
}

// Option represents an option can for the generate command.
type Option func(*cliConfig)

// OptionIgnoreIdentities sets which non-private identities should be ignored.
func OptionIgnoreIdentities(identities ...elemental.Identity) Option {
	return func(g *cliConfig) {

		var m = make(map[string]struct{}, len(identities))
		for _, i := range identities {
			m[i.Name] = struct{}{}
		}

		g.ignoredIdentities = m
	}
}

// OptionArgumentsPrefix sets which non-private identities should be ignored.
func OptionArgumentsPrefix(prefix string) Option {
	return func(g *cliConfig) {

		g.argumentsPrefix = prefix
	}
}

// ManipulatorMaker returs a function which can create a manipulator based on pflags.
type ManipulatorMaker = func(opts ...maniphttp.Option) (manipulate.Manipulator, error)

// ManipulatorMakerFromFlags returns a func that creates a manipulator based on command flags. Command flags are read using viper.
// It needs the following flags: FlagAPI, FlagToken, FlagAppCredentials, FlagNamespace, FlagCACertPath, FlagAPISkipVerify, FlagEncoding
// Use SetCLIFlags to add these flags to your command.
func ManipulatorMakerFromFlags(options ...maniphttp.Option) ManipulatorMaker {

	return func(innerOptions ...maniphttp.Option) (manipulate.Manipulator, error) {
		api := viper.GetString(flagAPI)
		token := viper.GetString(flagToken)
		namespace := viper.GetString(flagNamespace)
		capath := os.ExpandEnv(viper.GetString(flagCACertPath))
		skip := viper.GetBool(flagAPISkipVerify)
		encoding := viper.GetString(flagEncoding)

		var enc elemental.EncodingType
		switch encoding {
		case "json":
			enc = elemental.EncodingTypeJSON
		case "msgpack":
			enc = elemental.EncodingTypeMSGPACK
		default:
			return nil, fmt.Errorf("unsupported encoding '%s'. Must be 'json' or 'msgpack'", encoding)
		}

		rootCAPool, err := prepareAPICACertPool(capath)
		if err != nil {
			return nil, fmt.Errorf("unable to load root ca pool: %s", err)
		}

		/* #nosec */
		tlsConfig := &tls.Config{
			MinVersion:         tls.VersionTLS13,
			InsecureSkipVerify: skip,
			RootCAs:            rootCAPool,
		}

		opts := []maniphttp.Option{
			maniphttp.OptionNamespace(namespace),
			maniphttp.OptionTLSConfig(tlsConfig),
			maniphttp.OptionEncoding(enc),
			maniphttp.OptionToken(token),
		}

		opts = append(opts, options...)
		opts = append(opts, innerOptions...)

		return maniphttp.New(
			context.Background(),
			api,
			opts...,
		)
	}
}

// ManipulatorFlagSet returns the flagSet required to call ManipulatorFromFlags.
func ManipulatorFlagSet() *pflag.FlagSet {

	set := pflag.NewFlagSet("", pflag.ExitOnError)

	set.StringP(flagAPI, "A", "", "Server API URL.") // default is managed inline.
	set.BoolP(flagAPISkipVerify, "", false, "If set, skip api endpoint verification. This is insecure.")
	set.String(flagCACertPath, "", "Path to the CA to use for validating api endpoint.")
	set.String(flagTrackingID, "", "ID to trace the request. Use this when asked to help debug the system.")
	set.String(flagEncoding, "msgpack", "encoding to use to communicate with the platform. Can be 'msgpack' or 'json'")
	set.StringP(flagNamespace, "n", "/", "Namespace to use.")
	set.StringP(flagToken, "t", "", "JWT Token to use")

	return set
}

// New generates the API commands and subcommands based on the model manager.
func New(modelManager elemental.ModelManager, manipulatorMaker ManipulatorMaker, options ...Option) *cobra.Command {

	cfg := &cliConfig{}

	for _, opt := range options {
		opt(cfg)
	}

	rootCmd := &cobra.Command{
		Use:   "api [command] [flags]",
		Short: "Interact with resources and APIs",
	}
	rootCmd.PersistentFlags().StringP(flagOutput, "o", "default", "Format to used print output. Options are 'table', 'json', 'yaml', 'none', 'template' or 'default'.")
	rootCmd.PersistentFlags().StringSliceP(formatTypeColumn, "c", nil, "Only show the given columns. Only valid when '--output=table'.")
	rootCmd.PersistentFlags().StringSliceP(flagParameters, "p", nil, "Additional parameter to the request, in the form of key=value.")

	createCmd := &cobra.Command{
		Use:   "create",
		Short: "Create a new object",
	}

	updateCmd := &cobra.Command{
		Use:   "update",
		Short: "Update an object",
	}

	deleteCmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete an object",
	}

	deleteManyCmd := &cobra.Command{
		Use:   "delete-many",
		Short: "Delete multiple objects",
	}

	getCmd := &cobra.Command{
		Use:   "get",
		Short: "Get a single object",
	}

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List objects",
	}

	countCmd := &cobra.Command{
		Use:   "count",
		Short: "Count objects",
	}

	// Generate subcommands for each identity
	for _, identity := range modelManager.AllIdentities() {

		if _, ok := cfg.ignoredIdentities[identity.Name]; ok {
			continue
		}

		if identity.Private || identity.Name == "root" {
			continue
		}

		if cmd, err := generateCreateCommandForIdentity(identity, modelManager, manipulatorMaker, optionArgumentsPrefix(cfg.argumentsPrefix)); err == nil {
			createCmd.AddCommand(cmd)
		} else {
			slog.Debug("unable to generate create command for identity",
				"identity", identity.Name,
				err,
			)
		}

		if cmd, err := generateUpdateCommandForIdentity(identity, modelManager, manipulatorMaker, optionArgumentsPrefix(cfg.argumentsPrefix)); err == nil {
			updateCmd.AddCommand(cmd)
		} else {
			slog.Debug("unable to generate update command for identity",
				"identity", identity.Name,
				err,
			)
		}

		if cmd, err := generateDeleteCommandForIdentity(identity, modelManager, manipulatorMaker); err == nil {
			deleteCmd.AddCommand(cmd)
		} else {
			slog.Debug("unable to generate delete command for identity",
				"identity", identity.Name,
				err,
			)
		}

		if cmd, err := generateDeleteManyCommandForIdentity(identity, modelManager, manipulatorMaker); err == nil {
			deleteManyCmd.AddCommand(cmd)
		} else {
			slog.Debug("unable to generate delete-many command for identity",
				"identity", identity.Name,
				err,
			)
		}

		if cmd, err := generateGetCommandForIdentity(identity, modelManager, manipulatorMaker); err == nil {
			getCmd.AddCommand(cmd)
		} else {
			slog.Debug("unable to generate get command for identity",
				"identity", identity.Name,
				err,
			)
		}

		if cmd, err := generateListCommandForIdentity(identity, modelManager, manipulatorMaker); err == nil {
			listCmd.AddCommand(cmd)
		} else {
			slog.Debug("unable to generate list command for identity",
				"identity", identity.Name,
				err,
			)
		}

		if cmd, err := generateCountCommandForIdentity(identity, modelManager, manipulatorMaker); err == nil {
			countCmd.AddCommand(cmd)
		} else {
			slog.Debug("unable to generate count command for identity",
				"identity", identity.Name,
				err,
			)
		}
	}

	listenCmd, err := generateListenCommand(modelManager, manipulatorMaker)
	if err != nil {
		slog.Debug("unable to generate listen command for identity", err)
	}

	rootCmd.AddCommand(
		createCmd,
		updateCmd,
		deleteCmd,
		deleteManyCmd,
		getCmd,
		listCmd,
		countCmd,
		listenCmd,
	)

	return rootCmd
}
