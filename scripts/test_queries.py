import asyncio
from flow.flow_config import FlowConfig

async def test_queries():
    flow = FlowConfig('flow_config.json')
    
    # Test cases
    test_questions = [
        "total sales North",
        "Show total sales for North region",
        "What are the total sales in South region",
        "Show me sales details for East region",
        "List all orders from West region"
    ]
    
    for question in test_questions:
        print(f"\nTesting question: {question}")
        try:
            results = await flow.execute({"question": question})
            print("Results:", results)
        except Exception as e:
            print(f"Error: {str(e)}")

if __name__ == "__main__":
    asyncio.run(test_queries()) 