import gradio as gr
import pandas as pd
import os
import backend_logic as bl

def process_and_attribute(order_file, call_file):
    try:
        o_df = pd.read_csv(order_file.name)
        c_df = pd.read_csv(call_file.name)
        
        result = bl.run_attribution_process(o_df, c_df)
        
        # Clean up output columns for the manager's view
        display_cols = ['Order ID', 'Order Value', 'Order Time', 'Order Phone', 'Agent', 'Window A Time', 'Window B Time', 'Attributed Revenue']
        return result[display_cols], "✅ Attribution successful. Transpose complete."
    except Exception as e:
        return None, f"❌ Error: {str(e)}"

with gr.Blocks(title="TSC Revenue Hub") as demo:
    gr.Markdown("# 🚀 TSC Revenue Attribution Engine")
    with gr.Row():
        ord_in = gr.File(label="Upload Orders CSV")
        call_in = gr.File(label="Upload Calls CSV")
    
    btn = gr.Button("Run Transpose & Attribute", variant="primary")
    status = gr.Markdown()
    table = gr.Dataframe()

    btn.click(process_and_attribute, inputs=[ord_in, call_input], outputs=[table, status])

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 7860))
    demo.launch(server_name="0.0.0.0", server_port=port)
