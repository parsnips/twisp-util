package util

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	dynamodbv1 "github.com/aws/aws-sdk-go/service/dynamodb"
)

func toLambdaDynamoDBAttribute(a types.AttributeValue) events.DynamoDBAttributeValue {
	switch v := a.(type) {
	case *types.AttributeValueMemberB:
		return events.NewBinaryAttribute(v.Value)
	case *types.AttributeValueMemberBOOL:
		return events.NewBooleanAttribute(v.Value)
	case *types.AttributeValueMemberBS:
		return events.NewBinarySetAttribute(v.Value)
	case *types.AttributeValueMemberL:
		var avs = make([]events.DynamoDBAttributeValue, len(v.Value))
		for k, v := range v.Value {
			avs[k] = toLambdaDynamoDBAttribute(v)
		}
		return events.NewListAttribute(avs)
	case *types.AttributeValueMemberM:
		var avs = make(map[string]events.DynamoDBAttributeValue, len(v.Value))
		for k, v := range v.Value {
			avs[k] = toLambdaDynamoDBAttribute(v)
		}
		return events.NewMapAttribute(avs)
	case *types.AttributeValueMemberN:
		return events.NewNumberAttribute(v.Value)
	case *types.AttributeValueMemberNS:
		return events.NewNumberSetAttribute(v.Value)
	case *types.AttributeValueMemberNULL:
		return events.NewNullAttribute()
	case *types.AttributeValueMemberS:
		return events.NewStringAttribute(v.Value)
	case *types.AttributeValueMemberSS:
		return events.NewStringSetAttribute(v.Value)
	default:
		panic(fmt.Sprintf("unknown type %v", a))
	}
}

func ToLambdaDynamoDB(attributes map[string]types.AttributeValue) map[string]events.DynamoDBAttributeValue {
	var c = make(map[string]events.DynamoDBAttributeValue, len(attributes))
	for k, v := range attributes {
		c[k] = toLambdaDynamoDBAttribute(v)
	}
	return c
}

func ToDynamoDBAttributeValue(val *dynamodbv1.AttributeValue) (*events.DynamoDBAttributeValue, error) {
	var m map[string]any
	if err := CopyByJson(val, &m); err != nil {
		return nil, err
	}
	for k, v := range m {
		if v == nil {
			delete(m, k)
		}
	}
	var ev events.DynamoDBAttributeValue
	return &ev, CopyByJson(m, &ev)
}

func ToV2AttributeValueMap(m map[string]*dynamodbv1.AttributeValue) (map[string]types.AttributeValue, error) {
	var out = make(map[string]types.AttributeValue, len(m))
	for k, v := range m {
		if vv, err := toV2AttributeValue(v); err != nil {
			return nil, err
		} else {
			out[k] = vv
		}
	}
	return out, nil
}

func toV2AttributeValue(v *dynamodbv1.AttributeValue) (types.AttributeValue, error) {
	if v == nil {
		return nil, nil
	} else if ev, err := ToDynamoDBAttributeValue(v); err != nil {
		return nil, err
	} else if ev == nil {
		return nil, fmt.Errorf("Unexpected nil dynamodb attribute value")
	} else {
		return ToSDKDynamoDBAttribute(*ev), nil
	}
}

func ToSDKDynamoDBAttribute(a events.DynamoDBAttributeValue) types.AttributeValue {
	switch a.DataType() {
	case events.DataTypeBinary:
		return &types.AttributeValueMemberB{Value: a.Binary()}
	case events.DataTypeBoolean:
		return &types.AttributeValueMemberBOOL{Value: a.Boolean()}
	case events.DataTypeBinarySet:
		return &types.AttributeValueMemberBS{Value: a.BinarySet()}
	case events.DataTypeList:
		L := a.List()
		NEW := make([]types.AttributeValue, len(L))
		for i, v := range L {
			NEW[i] = ToSDKDynamoDBAttribute(v)
		}
		return &types.AttributeValueMemberL{Value: NEW}
	case events.DataTypeMap:
		M := a.Map()
		NEW := make(map[string]types.AttributeValue, len(M))
		for n, v := range M {
			NEW[n] = ToSDKDynamoDBAttribute(v)
		}
		return &types.AttributeValueMemberM{Value: NEW}
	case events.DataTypeNumber:
		return &types.AttributeValueMemberN{Value: a.Number()}
	case events.DataTypeNumberSet:
		return &types.AttributeValueMemberNS{Value: a.NumberSet()}
	case events.DataTypeNull:
		return &types.AttributeValueMemberNULL{Value: a.IsNull()}
	case events.DataTypeString:
		return &types.AttributeValueMemberS{Value: a.String()}
	case events.DataTypeStringSet:
		return &types.AttributeValueMemberSS{Value: a.StringSet()}
	default:
		panic(fmt.Sprintf("unknown type %v", a.DataType()))
	}
}

func ToSDKDynamoDB(attributes map[string]events.DynamoDBAttributeValue) map[string]types.AttributeValue {
	c := make(map[string]types.AttributeValue, len(attributes))
	for n, a := range attributes {
		c[n] = ToSDKDynamoDBAttribute(a)
	}
	return c
}

func CopyByJson(source, dest any) error {
	if bytes, err := json.Marshal(source); err != nil {
		return err
	} else {
		return json.Unmarshal(bytes, dest)
	}
}
