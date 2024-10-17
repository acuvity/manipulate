package manipcli

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.acuvity.ai/elemental"
	"go.acuvity.ai/manipulate"
	"go.acuvity.ai/manipulate/maniphttp"
)

// generateListenCommandForIdentity generates the command to listen for events based on its identity.
func generateListenCommand(modelManager elemental.ModelManager, manipulatorMaker ManipulatorMaker) (*cobra.Command, error) {

	cmd := &cobra.Command{
		Use:   "listen",
		Short: "Listen for events",
		RunE: func(cmd *cobra.Command, args []string) error {

			fRecursive := viper.GetBool(flagRecursive)
			fOutput := viper.GetString(flagOutput)
			fFormatTypeColumn := viper.GetStringSlice(formatTypeColumn)
			fOutputTemplate := viper.GetString(flagOutputTemplate)

			manipulator, err := manipulatorMaker()
			if err != nil {
				return fmt.Errorf("unable to make manipulator: %w", err)
			}

			subscriber := maniphttp.NewSubscriber(
				manipulator,
				maniphttp.SubscriberOptionRecursive(fRecursive),
				maniphttp.SubscriberOptionSupportErrorEvents(),
			)

			var filter *elemental.PushConfig
			filterIdentities := viper.GetStringSlice("identity")

			if len(filterIdentities) > 0 {

				filter = elemental.NewPushConfig()

				for _, i := range filterIdentities {
					identity := modelManager.IdentityFromAny(i)
					if identity.IsEmpty() {
						return fmt.Errorf("unknown identity %s", i)
					}
					filter.FilterIdentity(identity.Name)
				}
			}

			outputType := fOutput
			if fOutput == flagOutputDefault {
				outputType = flagOutputJSON
			}
			outputFormat := prepareOutputFormat(outputType, formatTypeHash, fFormatTypeColumn, fOutputTemplate)

			var once sync.Once
			terminated := make(chan struct{})

			go func() {

				pullErrorIfAny := func() error {
					select {
					case err := <-subscriber.Errors():
						return err
					default:
						return nil
					}
				}

				for {
					select {

					case evt := <-subscriber.Events():
						result, err := formatEvents(outputFormat, false, evt)
						if err != nil {
							slog.Error("unable to format event", err)
						}

						fmt.Fprint(cmd.OutOrStdout(), result)

					case st := <-subscriber.Status():
						switch st {
						case manipulate.SubscriberStatusInitialConnection:
							slog.Debug("status update", "status", "connected")
						case manipulate.SubscriberStatusInitialConnectionFailure:
							slog.Warn("status update", "status", "connect failed", pullErrorIfAny())
						case manipulate.SubscriberStatusDisconnection:
							slog.Warn("status update", "status", "disconnected", pullErrorIfAny())
						case manipulate.SubscriberStatusReconnection:
							slog.Info("status update", "status", "reconnected")
						case manipulate.SubscriberStatusReconnectionFailure:
							slog.Debug("status update", "status", "reconnection failed", pullErrorIfAny())
						case manipulate.SubscriberStatusFinalDisconnection:
							slog.Debug("status update", "status", "terminated")
							once.Do(func() { close(terminated) })
						}

					case err := <-subscriber.Errors():
						slog.Error("Error received", err)
					}
				}
			}()

			ctx, cancel := context.WithCancel(cmd.Context())
			defer cancel()
			subscriber.Start(ctx, filter)

			c := make(chan os.Signal, 1)
			signal.Reset(os.Interrupt)
			signal.Notify(c, os.Interrupt)

			select {
			case <-c:
				cancel()
			case <-cmd.Context().Done():
				fmt.Println("command context done")
			}

			<-terminated

			return nil
		},
	}

	cmd.Flags().BoolP(flagRecursive, "r", false, "Listen to all events in the current namespace and all child namespaces.")
	cmd.Flags().StringSliceP("identity", "i", []string{}, "Only display events for the given identities.")

	return cmd, nil
}
