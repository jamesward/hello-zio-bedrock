# Hello ZIO Bedrock

1. [Create a Bedrock Bearer token](https://us-east-1.console.aws.amazon.com/bedrock/home?region=us-east-1#/api-keys/long-term/create)
2. Set the auth token: `export AWS_BEARER_TOKEN_BEDROCK=YOUR_TOKEN`
3. Set the model: `export BEDROCK_MODEL_ID=us.anthropic.claude-sonnet-4-5-20250929-v1:0`
4. Run: `./sbt run`
