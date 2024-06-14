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
				Description: "The specified name of the flow.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("FlowName"),
			},
			{
				Name:        "created_at",
				Description: "Specifies when the flow was created.",
				Type:        proto.ColumnType_TIMESTAMP,
				Transform:   transform.FromField("CreatedAt"),
			},
			{
				Name:        "created_by",
				Description: "The ARN of the user who created the flow.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "description",
				Description: "A user-entered description of the flow.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "destination_connector_label",
				Description: "The label of the destination connector in the flow.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "destination_connector_type",
				Description: "Specifies the destination connector type, such as Salesforce, Amazon S3, Amplitude, and so on.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "status",
				Description: "Indicates the current status of the flow.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("FlowStatus"),
			},
			{
				Name:        "most_recent_execution_message",
				Description: "Describes the details of the most recent flow run.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("LastRunExecutionDetails.MostRecentExecutionMessage"),
			},
			{
				Name:        "most_recent_execution_status",
				Description: "Specifies the status of the most recent flow run.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("LastRunExecutionDetails.MostRecentExecutionStatus"),
			},
			{
				Name:        "most_recent_execution_time",
				Description: "Specifies the time of the most recent flow run.",
				Type:        proto.ColumnType_TIMESTAMP,
				Transform:   transform.FromField("LastRunExecutionDetails.MostRecentExecutionTime"),
			},
			{
				Name:        "last_updated_at",
				Description: "Specifies when the flow was last updated.",
				Type:        proto.ColumnType_TIMESTAMP,
				Transform:   transform.FromField("LastUpdatedAt"),
			},
			{
				Name:        "last_updated_by",
				Description: "Specifies the account user name that most recently updated the flow.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "source_connector_label",
				Description: "The label of the source connector in the flow.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "source_connector_type",
				Description: "Specifies the source connector type, such as Salesforce, Amazon S3, Amplitude, and so on.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "trigger_type",
				Description: "Specifies the type of flow trigger. This can be OnDemand , Scheduled , or Event.",
				Type:        proto.ColumnType_STRING,
			},
		}),
	}
}

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
