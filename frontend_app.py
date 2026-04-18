import gradio as gr
import pandas as pd
import backend_logic as bl

def process_file(file):
    try:
        # Load the formatted CSV
        raw_df = pd.read_csv(file.name)
        
        # Run logic
        result_df = bl.run_attribution(raw_df)
        
        # Select and order columns for the final report
        # Ensuring both Window A and Window B are shown for talk time reporting
        report_cols = [
            'Name', 'Total', 'Created at', 'Phone', 'Agent Name', 
            'Window A Time', 'Window B Time', 'Attributed Revenue'
        ]
        
        # Only keep columns that exist in the result
        final_report = result_df[[c for c in report_cols if c in result_df.columns]]
        
        return final_report, "✅ Attribution Complete."
    except Exception as e:
        return None, f"❌ Error: {str(e)}"

# UI Layout
with gr.Blocks(title="TSC Revenue Attribution") as demo:
    gr.Markdown("# 💰 Revenue Attribution Portal")
    gr.Markdown("Upload your formatted CSV to process duplication and proportionate splits.")
    
    with gr.Row():
        file_input = gr.File(label="Upload Formatted CSV")
    
    run_btn = gr.Button("Calculate Attribution", variant="primary")
    
    status = gr.Markdown()
    output_table = gr.Dataframe(label="Attributed Orders List")

    run_btn.click(
        fn=process_file,
        inputs=file_input,
        outputs=[output_table, status]
    )

if __name__ == "__main__":
    demo.launch()
