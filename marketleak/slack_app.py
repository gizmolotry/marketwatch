import os
import re
from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

# Ensure environment is loaded
load_dotenv()

# Initialize the Slack App with the Bot Token
app = App(token=os.environ.get("SLACK_BOT_TOKEN"))

@app.event("app_mention")
def handle_app_mention_events(body, say):
    """
    Handles when someone tags @MarketWatch in a channel.
    Routes to SynthesisAgent to generate a SAR output based on latest anomalies.
    """
    text = body["event"]["text"]
    user = body["event"]["user"]
    
    say(f"Analyzing the latest on-chain activity for you, <@{user}>... Please hold on!")
    
    try:
        from marketleak.agents.synthesis_agent import SynthesisAgent
        agent = SynthesisAgent()
        response = agent.generate_sar()
        
        # Slack has a limit of 4000 characters per message, so we might need to truncate
        if len(response) > 3900:
            response = response[:3900] + "\n...[Truncated]"
            
        say(response)
    except Exception as e:
        say(f"Sorry, I encountered an error while synthesizing the data: {str(e)}")

@app.command("/market-anomalies")
def handle_market_anomalies(ack, respond, command):
    """
    Slash command to get the top current anomalies.
    """
    ack()
    
    try:
        import pandas as pd
        from marketleak.scoring import get_top_anomalies
        cache_path = "demo_data/cache_anomalies.parquet"
        if not os.path.exists(cache_path):
            respond("Anomaly cache not built yet. Please run master_daemon.py.")
            return
            
        anomalies_df = pd.read_parquet(cache_path)
        if anomalies_df is None or anomalies_df.empty:
            respond("No significant anomalies detected at this time.")
            return
            
        top_anomalies = get_top_anomalies(anomalies_df, top_n=5)
        
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "🚨 Top Market Anomalies (Belief Shocks) 🚨"
                }
            },
            {
                "type": "divider"
            }
        ]
        
        for _, row in top_anomalies.iterrows():
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{row['question']}*\n• Shock Score: `{row['belief_shock']:.4f}`\n• Max Volume: `{row['max_volume']:.2f}`\n• Max Trade Size: `{row['max_trade_size']:.2f}`"
                }
            })
            
        respond(blocks=blocks)
    except Exception as e:
        respond(f"Error fetching anomalies: {str(e)}")

@app.command("/proxy-map")
def handle_proxy_map(ack, respond, command):
    """
    Slash command to map a wallet and see if it's in a proxy cluster.
    """
    ack()
    
    wallet = command.get("text", "").strip().lower()
    if not wallet:
        respond("Please provide a wallet address. Example: `/proxy-map 0xabc...`")
        return
        
    try:
        import pickle
        cache_path = "demo_data/cache_clusters.pkl"
        if not os.path.exists(cache_path):
            respond("Cluster cache not built yet. Please run master_daemon.py.")
            return
            
        with open(cache_path, "rb") as f:
            proxies = pickle.load(f)
        
        # Check if wallet is in the proxy map
        # Normalize wallet address removing 'wallet:' if needed
        clean_wallet = wallet.replace("wallet:", "")
        
        if clean_wallet in proxies:
            cluster_id = proxies[clean_wallet]
            # Find all wallets in this cluster
            cluster_wallets = [w for w, cid in proxies.items() if cid == cluster_id]
            
            respond(f"🚨 *Proxy Ring Detected!*\nWallet `{clean_wallet}` is part of cluster entity `{cluster_id}`.\nThere are {len(cluster_wallets)} total wallets in this cluster.")
        else:
            respond(f"✅ Wallet `{clean_wallet}` does not appear to be part of a known proxy ring based on current funding overlap.")
            
    except Exception as e:
        respond(f"Error checking proxy map: {str(e)}")

if __name__ == "__main__":
    app_token = os.environ.get("SLACK_APP_TOKEN")
    if not app_token:
        print("Error: SLACK_APP_TOKEN environment variable not set.")
        exit(1)
        
    print("Starting MarketWatch Slack App in Socket Mode...")
    handler = SocketModeHandler(app, app_token)
    handler.start()
