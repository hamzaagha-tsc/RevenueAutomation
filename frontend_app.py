import gradio as gr
import pandas as pd
import os
import backend_logic as bl

def process_excel_sheets(excel_file):
    try:
        # 1. Load both sheets from the SINGLE uploaded file
        # Ensure your Excel sheet names are exactly 'Orders' and 'Calls'
        orders_df = pd.read_excel(excel_file.name, sheet_name='Orders')
        calls_df = pd.read_excel(excel_file.name, sheet_name='Calls')
        
        # 2. Run the attribution logic
        result = bl.run_attribution_process(orders_df, calls_df)
        
        # 3. Clean up the output columns
        display_cols = [
            'Order ID', 'Order Value', 'Order Time', 'Order Phone', 
            'Agent', 'Window A Time', 'Window B Time', 'Attributed Revenue'
        ]
        
        # Filter only columns that exist
        final_df = result[[c for c in display_cols if c in result.columns]]
        
        return final_df, "✅ Attribution successful! Both sheets processed."
    
    except Exception as e:
        return None, f"❌ Error: {str(e)}. (Make sure sheets are named 'Orders' and 'Calls')"

# UI Layout
with gr.Blocks(title="TSC Revenue Hub") as demo:
    gr.Markdown("# 🚀 TSC Revenue Attribution Engine")
    gr.Markdown("Upload your **Single Excel File** containing the 'Orders' and 'Calls' sheets.")
    
    with gr.Row():
        file_input = gr.File(label="Upload Gemini1.xlsx", file_types=[".xlsx"])
    
    btn = gr.Button("Run Transpose & Attribute", variant="primary")
    status = gr.Markdown()
    table = gr.Dataframe()

    btn.click(
        fn=process_excel_sheets, 
        inputs=file_input, 
        outputs=[table, status]
    )

if __name__ == "__main__":
    # Correct port binding for Render
    port = int(os.environ.get("PORT", 7860))
    demo.launch(server_name="0.0.0.0", server_port=port)
