package aws

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/appflow"
	appflowv1 "github.com/aws/aws-sdk-go/service/appflow"
	"github.com/aws/smithy-go" // do we need this?
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
)

//// TABLE DEFINITION

func tableAwsAppFlowFlow(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "aws_appflow_flow",
		Description: "AWS AppFlow flow",
		List: &plugin.ListConfig{
			Hydrate: listAppFlowFlows,
			Tags:    map[string]string{"service": "appflow", "action": "ListFlows"},
			IgnoreConfig: &plugin.IgnoreConfig{
				ShouldIgnoreErrorFunc: shouldIgnoreErrors([]string{"ResourceNotFoundException"}),
			},
			KeyColumns: []*plugin.KeyColumn{
				{
					Name:    "name",
					Require: plugin.Optional,
				},
			},
		},
		HydrateConfig: []plugin.HydrateConfig{
			{
				Func: getAppStreamFleetTags,
				Tags: map[string]string{"service": "appstream", "action": "ListTagsForResource"},
			},
		},
		GetMatrixItemFunc: SupportedRegionMatrix(appflowv1.EndpointsID),
		Columns: awsRegionalColumns([]*plugin.Column{
			{
				Name:        "name",
				Description: "The name of the flow.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("FlowName"),
			},
		}),
	}
}

// func tableAwsStepFunctionsStateMachine(_ context.Context) *plugin.Table {
// 	return &plugin.Table{
// 		Name:        "aws_sfn_state_machine",
// 		Description: "AWS Step Functions State Machine",
// 		Get: &plugin.GetConfig{
// 			KeyColumns: plugin.SingleColumn("arn"),
// 			IgnoreConfig: &plugin.IgnoreConfig{
// 				ShouldIgnoreErrorFunc: shouldIgnoreErrors([]string{"ResourceNotFoundException", "StateMachineDoesNotExist", "InvalidArn"}),
// 			},
// 			Hydrate: getStepFunctionsStateMachine,
// 			Tags:    map[string]string{"service": "states", "action": "DescribeStateMachine"},
// 		},
// 		List: &plugin.ListConfig{
// 			Hydrate: listStepFunctionsStateMachines,
// 			Tags:    map[string]string{"service": "states", "action": "ListStateMachines"},
// 		},
// 		HydrateConfig: []plugin.HydrateConfig{
// 			{
// 				Func: getStepFunctionsStateMachine,
// 				Tags: map[string]string{"service": "states", "action": "DescribeStateMachine"},
// 			},
// 			{
// 				Func: getStepFunctionStateMachineTags,
// 				Tags: map[string]string{"service": "states", "action": "ListTagsForResource"},
// 			},
// 		},
// 		GetMatrixItemFunc: SupportedRegionMatrix(sfnv1.EndpointsID),
// 		Columns: awsRegionalColumns([]*plugin.Column{
// {
// 	Name:        "arn",
// 	Description: "The Amazon Resource Name (ARN) that identifies the state machine.",
// 	Type:        proto.ColumnType_STRING,
// 	Transform:   transform.FromField("StateMachineArn"),
// },
// {
// 	Name:        "status",
// 	Description: "The current status of the state machine.",
// 	Type:        proto.ColumnType_STRING,
// 	Hydrate:     getStepFunctionsStateMachine,
// },
// {
// 	Name:        "type",
// 	Description: "The type of the state machine.",
// 	Type:        proto.ColumnType_STRING,
// },
// {
// 	Name:        "creation_date",
// 	Description: "The date the state machine is created.",
// 	Type:        proto.ColumnType_TIMESTAMP,
// },
// {
// 	Name:        "definition",
// 	Description: "The Amazon States Language definition of the state machine.",
// 	Type:        proto.ColumnType_STRING,
// 	Hydrate:     getStepFunctionsStateMachine,
// },
// {
// 	Name:        "role_arn",
// 	Description: "The Amazon Resource Name (ARN) of the IAM role used when creating this state machine.",
// 	Type:        proto.ColumnType_STRING,
// 	Hydrate:     getStepFunctionsStateMachine,
// },
// {
// 	Name:        "logging_configuration",
// 	Description: "The LoggingConfiguration data type is used to set CloudWatch Logs options.",
// 	Type:        proto.ColumnType_JSON,
// 	Hydrate:     getStepFunctionsStateMachine,
// },
// {
// 	Name:        "tags_src",
// 	Description: "The list of tags associated with the state machine.",
// 	Type:        proto.ColumnType_JSON,
// 	Hydrate:     getStepFunctionStateMachineTags,
// 	Transform:   transform.FromValue(),
// },
// {
// 	Name:        "tracing_configuration",
// 	Description: "Selects whether AWS X-Ray tracing is enabled.",
// 	Type:        proto.ColumnType_JSON,
// 	Hydrate:     getStepFunctionsStateMachine,
// },

// Standard columns for all tables
// 			{
// 				Name:        "tags",
// 				Description: resourceInterfaceDescription("tags"),
// 				Type:        proto.ColumnType_JSON,
// 				Hydrate:     getAppFlowFlowTags,
// 				Transform:   transform.From(stateMachineTagsToTurbotTags),
// 			},
// 			{
// 				Name:        "title",
// 				Description: resourceInterfaceDescription("title"),
// 				Type:        proto.ColumnType_STRING,
// 				Transform:   transform.FromField("flowName"),
// 			},
// 			{
// 				Name:        "akas",
// 				Description: resourceInterfaceDescription("akas"),
// 				Type:        proto.ColumnType_JSON,
// 				Transform:   transform.FromField("flowArn").Transform(transform.EnsureStringArray),
// 			},
// 		}),
// 	}
// }

//// LIST FUNCTION

func listAppFlowFlows(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)

	// Create Session
	svc, err := AppFlowClient(ctx, d)
	if err != nil {
		logger.Error("aws_appflow_flow.listAppFlowFlows", "connection_error", err)
		return nil, err
	}

	// Unsupported region check
	if svc == nil {
		return nil, nil
	}

	params := &appflow.ListFlowsInput{}

	// if d.Quals["name"] != nil {
	// 	for _, q := range d.Quals["name"].Quals {
	// 		value := q.Value.GetStringValue()
	// 		if q.Operator == "=" {
	// 			params.Names = append(params.Names, value)
	// 		}
	// 	}
	// }

	pageLeft := true

	for pageLeft {
		// apply rate limiting
		d.WaitForListRateLimit(ctx)

		op, err := svc.ListFlows(ctx, params)

		if err != nil {
			logger.Error("aws_appflow_flow.listAppFlowFlows", "api_error", err)
			return nil, err
		}

		for _, fleet := range op.Flows {
			d.StreamListItem(ctx, fleet)

			// Context may get cancelled due to manual cancellation or if the limit has been reached
			if d.RowsRemaining(ctx) == 0 {
				return nil, nil
			}
		}

		if op.NextToken != nil {
			params.NextToken = op.NextToken
		} else {
			break
		}
	}

	return nil, nil
}

//// HYDRATE FUNCTIONS

func getAppFlowFlowTags(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)
	var arn string
	// if h.Item != nil {
	// 	arn = *h.Item.(types.Fleet).Arn
	// } else {
	// 	return nil, nil
	// }

	// Create Session
	svc, err := AppFlowClient(ctx, d)
	if err != nil {
		logger.Error("aws_appflow_flow.getAppFlowFlowTags", "connection_error", err)
		return nil, err
	}

	params := &appflow.ListTagsForResourceInput{
		ResourceArn: &arn,
	}

	tags, err := svc.ListTagsForResource(ctx, params)
	if err != nil {
		var ae smithy.APIError
		if errors.As(err, &ae) {
			if ae.ErrorCode() == "ResourceNotFoundException" {
				return nil, nil
			}
		}
		plugin.Logger(ctx).Error("aws_appflow_flow.getAppFlowFlowTags", "api_error", err)
		return nil, err
	}

	return tags.Tags, nil
}
