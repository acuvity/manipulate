package manipcli

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.acuvity.ai/elemental"
	"go.acuvity.ai/manipulate"
)

// generateDeleteManyCommandForIdentity generates the command to delete many objects based on its identity.
//
// Two mutually exclusive run modes:
//
//   - --parent: calls manipulator.DeleteMany directly. A single HTTP DELETE
//     on the parent-scoped collection URL, with query parameters from -p
//     riding along via mctx.Parameters(). This is the route for
//     narrowly-targeted collection-level deletes where the server identifies
//     the target via required query params (e.g. revoking a named token on a
//     parent resource).
//
//   - --filter (or neither flag): fetch-then-individual-delete loop. The
//     command RetrieveMany's matching objects then iterates and Delete's
//     each one. Requires --confirm. This is the historical behavior for
//     bulk-by-predicate deletes where per-object error reporting matters.
//
// Combining --parent with --filter is rejected: the two flags express
// distinct identification strategies (server-side query param vs.
// client-side predicate) and supporting their combination would mean
// fetch-loop semantics under a parent collection — a fourth shape that
// nothing currently asks for.
func generateDeleteManyCommandForIdentity(identity elemental.Identity, modelManager elemental.ModelManager, manipulatorMaker ManipulatorMaker) *cobra.Command {

	cmd := &cobra.Command{
		Use:     identity.Name,
		Aliases: []string{identity.Category},
		Short:   "Delete multiple " + identity.Category,
		// Aliases: TODO: Missing alias from the spec file -> To be stored in the identity ?,
		RunE: func(cmd *cobra.Command, args []string) error {

			fParam := viper.GetStringSlice("param")
			fTrackingID := viper.GetString(flagTrackingID)
			fConfirm := viper.GetBool(flagConfirm)
			fFilter := viper.GetString(flagFilter)
			fOutput := viper.GetString(flagOutput)
			fFormatTypeColumn := viper.GetStringSlice(formatTypeColumn)
			fOutputTemplate := viper.GetString(flagOutputTemplate)
			fNamespace := viper.GetString(flagNamespace)

			if viper.IsSet(flagParent) && fFilter != "" {
				return fmt.Errorf("--%s and --%s cannot be used together: --%s does a single collection-level DELETE (server identifies targets via -p query params), while --%s does a fetch-then-delete loop. Pick one", flagParent, flagFilter, flagParent, flagFilter)
			}

			manipulator, err := manipulatorMaker()
			if err != nil {
				return fmt.Errorf("unable to make manipulator: %w", err)
			}

			parameters, err := parametersToURLValues(fParam)
			if err != nil {
				return fmt.Errorf("unable to convert parameters to url values: %w", err)
			}

			options := []manipulate.ContextOption{
				manipulate.ContextOptionTracking(fTrackingID, "cli"),
				manipulate.ContextOptionParameters(parameters),
				manipulate.ContextOptionFields(fFormatTypeColumn),
				manipulate.ContextOptionOverride(fConfirm),
			}

			if viper.IsSet(flagParent) {
				parentName, parentID, err := splitParentInfo(viper.GetString(flagParent))
				if err != nil {
					return err
				}
				parent := modelManager.IdentifiableFromString(parentName)
				if parent == nil {
					return fmt.Errorf("unknown identity %s", parentName)
				}
				parent.SetIdentifier(parentID)
				options = append(options, manipulate.ContextOptionParent(parent))
			}

			if fFilter != "" {
				f, err := elemental.NewFilterFromString(fFilter)
				if err != nil {
					return fmt.Errorf("unable to parse filter %s: %w", fFilter, err)
				}
				options = append(options, manipulate.ContextOptionFilter(f))
			}

			ctx, cancel := context.WithTimeout(cmd.Context(), 60*time.Second)
			defer cancel()

			mctx := manipulate.NewContext(ctx, options...)

			// --parent → single collection-level DELETE via manipulator.DeleteMany.
			// The server identifies the target(s) via query params (`-p key=value`)
			// declared by the spec relation. The --parent + --filter combination
			// was rejected earlier, so reaching here without a filter is the only
			// possibility.
			if viper.IsSet(flagParent) {
				if err := manipulator.DeleteMany(mctx, identity); err != nil {
					return fmt.Errorf("unable to delete %s: %w", identity.Category, err)
				}

				outputType := fOutput
				if fOutput == flagOutputDefault {
					outputType = flagOutputNone
				}

				result, err := formatObjects(
					prepareOutputFormat(outputType, formatTypeArray, fFormatTypeColumn, fOutputTemplate),
					true,
				)

				if err != nil {
					return fmt.Errorf("unable to format output: %w", err)
				}

				_, _ = fmt.Fprint(cmd.OutOrStdout(), result)
				return nil
			}

			// --filter set → legacy fetch-then-individual-delete loop.
			identifiables := modelManager.Identifiables(identity)
			if err := manipulator.RetrieveMany(mctx, identifiables); err != nil {
				return fmt.Errorf("unable to retrieve %s: %w", identity.Category, err)
			}

			objects := identifiables.List()

			if !fConfirm {
				return fmt.Errorf("you are about to delete %d %s. If you are sure, please use --%s option to delete %v", len(objects), identity.Category, flagConfirm, fConfirm)
			}

			var deleted elemental.IdentifiablesList

			errs := elemental.NewErrors()
			for _, o := range objects {

				nsable, ok := o.(elemental.Namespaceable)
				if ok {
					mctx = mctx.Derive(manipulate.ContextOptionNamespace(nsable.GetNamespace()))
				} else {
					mctx = mctx.Derive(manipulate.ContextOptionNamespace(fNamespace))
				}

				if err := manipulator.Delete(mctx, o); err != nil {
					errs = errs.Append(err)
					continue
				}

				deleted = append(deleted, o)
			}

			if len(errs) > 0 {
				return fmt.Errorf("some %s were not deleted: %w", identity.Category, errs)
			}

			outputType := fOutput
			if fOutput == flagOutputDefault {
				outputType = flagOutputNone
			}

			result, err := formatObjects(
				prepareOutputFormat(outputType, formatTypeArray, fFormatTypeColumn, fOutputTemplate),
				true,
				deleted...,
			)

			if err != nil {
				return fmt.Errorf("unable to format output: %w", err)
			}

			_, _ = fmt.Fprint(cmd.OutOrStdout(), result)
			return nil
		},
	}

	cmd.Flags().StringP(flagFilter, "f", "", "Query filter.")
	cmd.Flags().BoolP(flagConfirm, "", false, "Confirm deletion of multiple objects")
	cmd.Flags().StringP(flagParent, "", "", "Provide information about parent resource. Format `name/ID`")

	return cmd
}
