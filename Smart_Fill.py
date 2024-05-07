import streamlit as st
import pdfplumber
from PIL import Image
from pdf2image import convert_from_bytes
import io


page_icon = Image.open("./favicon.ico")
st.set_page_config(page_title="Smart Fill", page_icon=page_icon, layout="wide", initial_sidebar_state="expanded")
logo = Image.open("./favicon.ico")

# Function to extract attributes from PDF document using pdfplumber
def extract_attributes_from_pdf(file_path, selected_attributes, attribute_mappings):
    attributes = {}
    with pdfplumber.open(file_path) as pdf:
        first_page = pdf.pages[0]  # Assuming attributes are on the first page
        text = first_page.extract_text()
        for user_friendly_attribute in selected_attributes:
            possible_attribute_names = attribute_mappings[user_friendly_attribute]
            for attribute_name in possible_attribute_names:
                if attribute_name in text:
                    attribute_index = text.index(attribute_name)
                    end_index = text.find("\n", attribute_index)
                    attribute_value = text[attribute_index + len(attribute_name):end_index].strip()
                    # Store using the attribute name as it appears in the document
                    attributes[attribute_name] = attribute_value
                    break  
    return attributes

# Function to render PDF preview
def render_pdf_preview(file_path):
    images = convert_from_bytes(file_path.read(), size=(800, None))
    return images

# Function to convert image to PDF
def convert_image_to_pdf(image_data):
    pdf_bytes = io.BytesIO()
    image_data.save(pdf_bytes, format='PDF')
    pdf_bytes.seek(0)
    return pdf_bytes

# Streamlit UI
# st.sidebar.image("FamiologyTextLogo.png", use_column_width=True)

st.markdown(
    """
    <style>
        section[data-testid="stSidebar"] {
            width: 400px !important; # Set the width to your desired value
        }
    </style>
    """,
    unsafe_allow_html=True,
)

def main():
    st.title("Smart Fill - Famiology.io")
    with st.sidebar.container():
        st.header("About App")
        # Section 1: Information about Smart Fill
        with st.expander("Smart Fill: Automatic Data Prepopulation", expanded=True):
            st.write("This intelligent feature automatically populates relevant data fields based on the document you upload. Say goodbye to manual data entry and let Smart Fill do the heavy lifting for you.")
        # Section 2: Usage Instructions
        with st.expander("How It Works?"):
            st.write("1. Upload Your Document: Simply upload your document using the provided file upload button.\n"
                     "2. Smart Analysis: Our system analyzes the document to identify key data points such as names, addresses, dates, and more. Further we can configure it with reference to domain.\n"
                     "3. Automatic Prepopulation: Once the analysis is complete, Smart Fill intelligently fills in the corresponding fields in your form, saving you time and effort.\n"
                     "4. Review and Edit: You always have the final say. Review the pre populated data, make any necessary edits, and proceed with confidence.")
        # Section 3: Configuration
        with st.sidebar.expander("Configuration:", expanded=True):
            attribute_mappings = {
                "Property": ["Property", "Property Address", "Address"],
                "Borrower(s)": ["Borrower", "Borrower(s)", "Applicant(s)"],
                "Seller(s)": ["Seller", "Seller(s)"],
                "Loan Amount": ["Loan Amount", "Amount"],
                "Loan Term": ["Loan Term", "Term"],
                "Lender": ["Lender", "Lender Name"],
                "Cash to Close": ["Cash to Close", "Closing Costs", "Cash Required"]
            }
            selected_attributes = st.multiselect("Select Attributes", list(attribute_mappings.keys()))
            if not selected_attributes:
                selected_attributes = list(attribute_mappings.keys())
        # Section 4: Additional Notes
        with st.expander("What problem it solves?"):
            st.write("Client Experience: Busy or unmotivated clients  expect automated pre-filled information sourced from AI or backend operations.\n"
                     "Efficiency: \n"
                     "Cut down on manual data entry and reduce processing time.")
        # Section 5: Value
        with st.expander("Value:"):
            st.write("The inherent value proposition of Smart Fill for users resides in its capacity to expedite and optimize data entry workflows through the automated population of pertinent fields, leveraging document uploads as the primary data source.")

    # File upload
    uploaded_file = st.file_uploader("Upload document", type=["pdf", "png", "jpg", "jpeg"])

    if uploaded_file:
        if uploaded_file.type in ['image/png', 'image/jpeg']:
            # Resize image
            image = Image.open(uploaded_file)
            image_resized = image.resize((800, 800))
            col1, col2 = st.columns(2)
            preview_clicked = col1.button("Preview")
            extract_clicked = col2.button("Extract Information")
            if extract_clicked:
                col1.image(image_resized, use_column_width=True, caption="Page Preview")
                pdf_data = convert_image_to_pdf(image_resized)
                pdf_image = render_pdf_preview(pdf_data)
                st.write("Image converted to PDF successfully!")
                # Extract attributes from PDF document using pdfplumber
                attributes = extract_attributes_from_pdf(pdf_data, selected_attributes, attribute_mappings)
                if attributes:
                    for key, value in attributes.items():
                        col2.text_input(key, value)  
                else:
                    st.info("No attributes identified.")
            elif preview_clicked:
                col1.image(image_resized, use_column_width=True, caption="Page Preview")
        elif uploaded_file.type == 'application/pdf':
            col1, col2 = st.columns(2)
            preview_clicked = col1.button("Preview")
            extract_clicked = col2.button("Extract Information")
            if extract_clicked:
                pdf_images = render_pdf_preview(uploaded_file)
                for image in pdf_images:
                    col1.image(image, use_column_width=True, caption="Page Preview")
                # Extract attributes from PDF document using pdfplumber
                attributes = extract_attributes_from_pdf(uploaded_file, selected_attributes, attribute_mappings)
                if attributes:
                    for key, value in attributes.items():
                        col2.text_input(key, value)  
                else:
                    st.info("No attributes identified.")
            elif preview_clicked:
                pdf_images = render_pdf_preview(uploaded_file)
                for image in pdf_images:
                    col1.image(image, use_column_width=True, caption="Page Preview")

if __name__ == "__main__":
    main()
