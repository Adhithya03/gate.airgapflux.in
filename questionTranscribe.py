import os
import json
import re
import google.generativeai as genai
from google.ai.generativelanguage_v1beta.types import content
from PIL import Image  # Import Pillow (PIL Fork) for image handling

def gemini_extract_question_data(image_path):
    """
    Extracts question data from the image at image_path using the Gemini API,
    following the specified schema.
    """
    try:
        genai.configure(api_key="")

        generation_config = {
            "temperature": 1.0,
            "max_output_tokens": 8192,
            "response_schema": content.Schema(
                type = content.Type.ARRAY,
                description = "Schema for extracting GATE EE questions from images using OCR. This schema prioritizes consistency and unambiguous formatting. When in doubt, ALWAYS use the more explicit MathJax formatting. **FAILURE TO USE MATHJAX CORRECTLY IS UNACCEPTABLE.**", # Stronger warning
                items = content.Schema(
                    type = content.Type.OBJECT,
                    enum = [],
                    required = ["question_number", "question_text", "question_type", "has_diagram"],
                    properties = {
                        "question_number": content.Schema(
                            type = content.Type.INTEGER,
                            description = "The question number on the page.",
                        ),
                        "question_text": content.Schema(
                            type = content.Type.STRING,
                            description = """The full text of the GATE question.

            **CRITICAL INSTRUCTIONS - MATHJAX FORMATTING IS MANDATORY AND MUST BE PERFECT:**

            1.  **ABSOLUTELY NO UNICODE FOR MATH:**  Do **NOT** use Unicode characters for mathematical symbols like degree (°), subscripts (₁), delta (δ, Δ), ohms (Ω), etc.  **UNICODE CHARACTERS ARE FORBIDDEN FOR MATHEMATICAL CONTENT.**

            2.  **USE ONLY STANDARD MATHJAX:** You **MUST** use standard MathJax syntax for **ALL** mathematical expressions, units, and symbols.

                -   **Correct MathJax Examples:**
                    -   Degree symbol: `\\(^\\circ\\)`  (e.g., `\\(90^\\circ\\)`)
                    -   Subscript 1: `\\(_1\\)` (e.g., `\\(V_1\\)`)
                    -   Lowercase delta: `\\(\\delta\\)`
                    -   Uppercase delta: `\\(\\Delta\\)`
                    -   Ohms: `\\(\\Omega\\)`
                    -   z inverse: `\\(z^{-1}\\)`

                -   **INCORRECT (DO NOT USE):**  `°`, `₁`, `δ`, `Δ`, `Ω`, or any other Unicode symbol for math.

            3.  **Units and Measurements:**  ALL units MUST be in MathJax (e.g., `\\(\\text{ V}\\)`, `\\(\\text{ A}\\)`, `\\(\\Omega\\)`).

            4.  **Mathematical Content:**  ALL mathematical symbols and expressions MUST be in MathJax (e.g., `\\(x=2\\)`, `\\(\\beta_F = 100\\)`).

            **FAILURE TO FOLLOW THESE MATHJAX INSTRUCTIONS WILL RESULT IN INCORRECT OUTPUT.**""", # Very strong warning
                        ),
                        "question_type": content.Schema(
                            type = content.Type.STRING,
                            description = """The type of question (MCQ or NAT).""",
                            enum = ["MCQ", "NAT"]
                        ),
                        "options": content.Schema(
                            type = content.Type.ARRAY,
                            description = """For MCQ questions, exactly four options.

            **INSTRUCTIONS - MATHJAX IN OPTIONS IS ALSO MANDATORY:**

            1.  **Option Format (Strict):** MUST be 'A) ', 'B) ', 'C) ', 'D) ' (capital letters, single space).

            2.  **MathJax in Options:**  ALL mathematical content within options **MUST ALSO** use MathJax, following the **SAME STRICT MATHJAX RULES** as in 'question_text' (no Unicode for math!).

            3.  **No Redundancy:** Options listed here MUST NOT be repeated or included in the 'question_text'. Options ONLY in this array.""", # Re-emphasize MathJax in options and no redundancy
                            items = content.Schema(
                                type = content.Type.STRING,
                                description = "Each option MUST start with 'A) ', 'B) ', 'C) ', 'D) ' and use MathJax for math.",
                            ),
                        ),
                        "has_diagram": content.Schema(
                            type = content.Type.BOOLEAN,
                            description = "True if the question has a diagram, table, or graph; otherwise false.",
                        ),
                        "numerical_answer": content.Schema(
                            type = content.Type.OBJECT,
                            description = "For NAT questions precision. Given in the question to round-off to.",
                            enum = [],
                            properties = {
                                "rounding": content.Schema(
                                    type = content.Type.STRING,
                                    description = "Rounding format ('1_decimal', '2_decimal', '3_decimal', 'integer'). Default '2_decimal'.",
                                    enum = ["1_decimal", "2_decimal", "3_decimal", "integer"]
                                )}
                        ),
                    },
                ),
            ),
            "response_mime_type": "application/json",
        }


        model = genai.GenerativeModel(
            model_name="gemini-1.5-flash",
            generation_config=generation_config,
        )

        pil_image = Image.open(image_path) # Load image using PIL

        contents = [
                    pil_image, # Pass PIL Image object directly
                    """
                    **EXTRACT GATE EE QUESTIONS WITH PERFECT MATHJAX FORMATTING - THIS IS CRITICAL.**

                    Follow these strict rules:

                    1.  **NO UNICODE MATH SYMBOLS:** Absolutely NO Unicode characters for math (degree, subscript, delta, ohms, etc.).

                    2.  **MANDATORY MATHJAX:** Use ONLY standard MathJax for ALL math, units, and symbols.

                        -   **Examples of Correct MathJax:**  `\\(^\\circ\\)`, `\\(_1\\)`, `\\(\\delta\\)`, `\\(\\Delta\\)`, `\\(\\Omega\\)`, `\\(z^{-1}\\)`.

                    3.  **Options Handling:**  Do NOT include options in 'question_text'. Options go ONLY in the 'options' array, formatted as 'A) ', 'B) ', 'C) ', 'D) ' with MathJax inside.

                    4.  **JSON Output:** Output the extracted questions in JSON format strictly according to the provided schema.

                    **YOUR OUTPUT MUST HAVE PERFECT MATHJAX AND NO UNICODE MATH SYMBOLS.  Double-check before outputting.**
                    """
                ]



        response = model.generate_content(contents=contents)
        response.resolve() # Resolve the response to get the content

        if response.text:
            try:
                question_data_list = json.loads(response.text)
                return question_data_list
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON response from Gemini: {e}")
                print(f"Response text was: {response.text[:200]}") # Print the raw response for debugging
                return None
        else:
            print("Gemini API returned an empty response text.")
            return None


    except Exception as e:
        print(f"Error during Gemini API call or processing: {e}")
        return None


def process_images(root_dir, year=None, output_file="output_temp_1.0.json", retry_short_content=True):
    """
    Processes images with validation to ensure question and option data is complete.

    Args:
        root_dir: Root directory containing year-based subdirectories of images
        year: Specific year directory to process (if None, processes all years)
        output_file: JSON file to store results
        retry_short_content: Whether to retry processing images with suspiciously short content
    """
    output_data = {}
    reprocess_list = []  # Track items that need reprocessing

    # Try to load existing data from JSON
    if os.path.exists(output_file):
        try:
            with open(output_file, 'r') as f:
                output_data = json.load(f)
                print(f"Resuming from existing JSON file: {output_file}")

                # Validate existing data first
                if retry_short_content:
                    for year_dir, pages in output_data.items():
                        for page_no, questions in pages.items():
                            should_reprocess = False

                            # Check for empty question lists
                            if not questions:
                                should_reprocess = True
                            else:
                                for q in questions:
                                    # Check for suspiciously short content
                                    if len(q.get('question_text', '')) < 5:
                                        should_reprocess = True
                                        break

                                    # Check if any option is suspiciously short
                                    for opt in q.get('options', []):
                                        if opt and len(opt) < 5:
                                            should_reprocess = True
                                            break

                            if should_reprocess:
                                reprocess_list.append((year_dir, page_no))
                                print(f"Flagging {year_dir}/{page_no} for reprocessing due to suspicious content")
        except json.JSONDecodeError:
            print(f"Warning: Existing JSON file '{output_file}' is corrupted or invalid. Starting fresh.")
            output_data = {}

    # Determine which year directories to process
    if year:
        if not os.path.isdir(os.path.join(root_dir, year)):
            print(f"Error: Year directory '{year}' not found in '{root_dir}'.")
            return output_data
        year_dirs = [year]
    else:
        year_dirs = [d for d in os.listdir(root_dir) if os.path.isdir(os.path.join(root_dir, d)) and d.isdigit()]

    # Process flagged reprocessing items first
    for year_dir_name, page_no in reprocess_list:
        if year_dir_name not in year_dirs:
            print(f"Skipping reprocess of {year_dir_name}/{page_no} as year not in scope")
            continue

        year_dir_path = os.path.join(root_dir, year_dir_name)
        # Find matching image file for this page
        image_files = [f for f in os.listdir(year_dir_path)
                      if f.lower().endswith(('.png', '.jpg', '.jpeg', '.tiff', '.bmp'))
                      and f.startswith(year_dir_name)
                      and re.search(r'_(' + page_no + r'|0*' + page_no + r')\.', f)]

        if not image_files:
            print(f"Warning: Could not find image file for {year_dir_name}/{page_no} for reprocessing")
            continue

        image_path = os.path.join(year_dir_path, image_files[0])
        print(f"Reprocessing {year_dir_name}/{page_no} ({image_files[0]})")

        # Try extraction with up to 3 retries
        success = False
        for attempt in range(3):
            try:
                question_data_list = gemini_extract_question_data(image_path)

                # Validate the newly extracted data
                if validate_question_data(question_data_list):
                    output_data[year_dir_name][page_no] = question_data_list
                    write_to_json(output_data, output_file=output_file)
                    print(f"Successfully reprocessed {year_dir_name}/{page_no} (Attempt {attempt+1})")
                    success = True
                    break
                else:
                    print(f"Reprocessing attempt {attempt+1} for {year_dir_name}/{page_no} produced invalid data, retrying...")
            except Exception as e:
                print(f"Error during reprocessing attempt {attempt+1} for {year_dir_name}/{page_no}: {str(e)}")

        if not success:
            print(f"Failed to reprocess {year_dir_name}/{page_no} after multiple attempts")

    # Process regular files
    for year_dir_name in sorted(year_dirs):
        if year_dir_name not in output_data:
            output_data[year_dir_name] = {}

        year_dir_path = os.path.join(root_dir, year_dir_name)
        image_files = [f for f in os.listdir(year_dir_path)
                      if f.lower().endswith(('.png', '.jpg', '.jpeg', '.tiff', '.bmp'))]

        for image_file in sorted(image_files):
            if not image_file.startswith(year_dir_name):
                print(f"Warning: Skipping image file '{image_file}' as filename doesn't start with year.")
                continue

            page_no_match = re.search(r'_(\d+)\.', image_file)
            if page_no_match:
                page_no = page_no_match.group(1).lstrip('0')
            else:
                print(f"Warning: Could not extract page number from filename '{image_file}'. Skipping.")
                continue

            # Skip if page already processed and not flagged for reprocessing
            if page_no in output_data[year_dir_name] and (year_dir_name, page_no) not in reprocess_list:
                print(f"Page {year_dir_name}/{page_no} already processed. Skipping.")
                continue

            image_path = os.path.join(year_dir_path, image_file)
            success = False

            # Try extraction with up to 3 retries
            for attempt in range(3):
                try:
                    question_data_list = gemini_extract_question_data(image_path)

                    # Validate the newly extracted data
                    if validate_question_data(question_data_list):
                        output_data[year_dir_name][page_no] = question_data_list
                        write_to_json(output_data, output_file=output_file)
                        print(f"Data for {year_dir_name}/{page_no} written to JSON (Attempt {attempt+1})")
                        success = True
                        break
                    else:
                        print(f"Processing attempt {attempt+1} for {year_dir_name}/{page_no} produced invalid data, retrying...")
                except Exception as e:
                    print(f"Error during processing attempt {attempt+1} for {year_dir_name}/{page_no}: {str(e)}")

            if not success:
                print(f"Failed to process {year_dir_name}/{page_no} after multiple attempts")
                # Still save what we have, but mark it as potentially problematic
                if question_data_list:
                    output_data[year_dir_name][page_no] = question_data_list
                    output_data[year_dir_name][page_no + "_needs_verification"] = True
                    write_to_json(output_data, output_file=output_file)

    return output_data

def validate_question_data(question_data_list):
    """
    Validates if the extracted question data meets quality standards.

    Args:
        question_data_list: List of question data dictionaries

    Returns:
        bool: True if data passes validation, False otherwise
    """
    if not question_data_list:
        return False

    for q in question_data_list:
        # Check question text
        if 'question_text' not in q or len(q['question_text']) < 5:
            return False

        # Check options - should have at least 2 options and they shouldn't be too short
        if 'options' not in q or len(q['options']) < 2:
            return False

        # Check that options aren't suspiciously short
        short_options = sum(1 for opt in q['options'] if opt and len(opt) < 5)
        if short_options > 1:  # Allow at most one short option (could be "Yes", "No", etc.)
            return False

    return True


def write_to_json(data, output_file="output_temp_1.0.json"):
    """
    Writes the processed data to a JSON file. This version overwrites the file each time.
    For incremental saving, it's called after each page is processed.
    """
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=4)
    print(f"JSON updated: {output_file}")  # More informative message

if __name__ == "__main__":
    root_directory = input("Enter the root directory containing year directories: ")
    process_specific_year = input("Process specific year? (Enter year or leave blank for all years): ")
    output_json_file = "output_temp_1.0.json"  # Define output JSON file name

    if not os.path.isdir(root_directory):
        print(f"Error: Root directory '{root_directory}' is not found.")
    else:
        processed_data = process_images(
            root_directory,
            year=process_specific_year if process_specific_year else None,
            output_file=output_json_file
        )
        print("Image processing and JSON generation completed.")
        print(f"Final output (also incrementally saved) is in: {output_json_file}")
