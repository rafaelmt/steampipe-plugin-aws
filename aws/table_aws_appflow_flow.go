package aws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/appflow"
	"github.com/aws/aws-sdk-go-v2/service/appflow/types"
	appflowv1 "github.com/aws/aws-sdk-go/service/appflow"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
)

//// TABLE DEFINITION

func tableAwsAppFlowFlow(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "aws_appflow_flow",
		Description: "AWS AppFlow flow",
		Get: &plugin.GetConfig{
			KeyColumns: plugin.SingleColumn("name"),
			IgnoreConfig: &plugin.IgnoreConfig{
				ShouldIgnoreErrorFunc: shouldIgnoreErrors([]string{"ResourceNotFoundException"}),
			},
			Hydrate: getAppFlowFlow,
			Tags:    map[string]string{"service": "appflow", "action": "DescribeFlow"},
		},
		List: &plugin.ListConfig{
			Hydrate: listAppFlowFlows,
			Tags:    map[string]string{"service": "appflow", "action": "ListFlows"},
			IgnoreConfig: &plugin.IgnoreConfig{
				ShouldIgnoreErrorFunc: shouldIgnoreErrors([]string{"ResourceNotFoundException"}),
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
				Name:        "arn",
				Description: "The flow's Amazon Resource Name (ARN).",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("FlowArn"),
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
				Hydrate:     getAppFlowFlow,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromValue().Transform(getDestinationFlowConnectorType),
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
				Hydrate:     getAppFlowFlow,
				Description: "Specifies the source connector type, such as Salesforce, Amazon S3, Amplitude, and so on.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("SourceFlowConfig.ConnectorType"),
			},
			{
				Name:        "source_connector_profile_name",
				Hydrate:     getAppFlowFlow,
				Description: "The name of the connector profile.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("SourceFlowConfig.ConnectorProfileName"),
			},
			{
				Name:        "trigger_type",
				Hydrate:     getAppFlowFlow,
				Description: "Specifies the type of flow trigger. This can be OnDemand , Scheduled , or Event.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("TriggerConfig.TriggerType"),
			},
			{
				Name:        "tasks",
				Hydrate:     getAppFlowFlow,
				Description: "???????",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Tasks"),
			},

			// Standard columns for all tables
			{
				Name:        "title",
				Description: resourceInterfaceDescription("title"),
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("FlowName"),
			},
			{
				Name:        "tags",
				Description: resourceInterfaceDescription("tags"),
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "akas",
				Description: resourceInterfaceDescription("akas"),
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("FlowArn").Transform(arnToAkas),
			},
		}),
	}
}

func getDestinationFlowConnectorType(_ context.Context, d *transform.TransformData) (interface{}, error) {
	destinationFlowConfigList := d.Value.(*appflow.DescribeFlowOutput).DestinationFlowConfigList
	// TODO maybe check this array before?
	data := &destinationFlowConfigList[0].ConnectorType

	return data, nil
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

func getAppFlowFlow(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)

	// Create Session
	svc, err := AppFlowClient(ctx, d)
	if err != nil {
		logger.Error("aws_appflow_flow.listAppFlowFlows", "connection_error", err)
		return nil, err
	}

	var name string
	if h.Item != nil {
		name = *h.Item.(types.FlowDefinition).FlowName
	} else {
		name = d.EqualsQuals["name"].GetStringValue()
	}

	params := &appflow.DescribeFlowInput{}
	params.FlowName = &name

	data, err := svc.DescribeFlow(ctx, params)
	if err != nil {
		plugin.Logger(ctx).Error("aws_appflow_flow_.describeFlow", "api_error", err)
		return nil, err
	}

	return data, nil
}
