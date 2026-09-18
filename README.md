# Hello ZIO Bedrock

1. [Create a Bedrock Bearer token](https://us-east-1.console.aws.amazon.com/bedrock/home?region=us-east-1#/api-keys/long-term/create).
2. Set Bedrock configuration:
   ```bash
   export AWS_BEARER_TOKEN_BEDROCK=YOUR_TOKEN
   export BEDROCK_MODEL_ID=us.anthropic.claude-sonnet-4-5-20250929-v1:0
   ```
3. Select an experiment mode:
   ```bash
   # Baseline: all javadocs.dev MCP tools are directly visible to Bedrock.
   ./sbt "run original"

   # Synthetic: one Bedrock tool recursively plans with Jev, filters with Jev,
   # and executes the generated MCP workflow internally. Backward planning is
   # the default; forward remains available for A/B testing.
   export TYPESAFE_API_KEY=YOUR_TYPESAFE_TOKEN
   ./sbt "run jev-synth"
   ./sbt "run jev-synth backward"
   ./sbt "run jev-synth forward"

   # Flipped orchestration: TypeSafeAI.loop owns planning, MCP executes
   # internally, and Bedrock is called once without tools for final prose.
   ./sbt "run jev-loop"
   ```

The experiment uses pinned releases of `zio-bedrock-converse` 0.1.1,
`zio-http-mcp` 0.8.2, and `zio-typesafe-ai` 0.0.2.
