"""
BDA Output Parser Module for Lambda
Adapted from parse_bedrock_enhanced.py for use in Lambda functions
"""

import json
from typing import Dict, List, Any, Tuple
from aws_lambda_powertools import Logger

logger = Logger()

class BedrockOutputParser:
    """Parser for Bedrock Data Automation output JSON."""
    
    def __init__(self):
        pass
    
    def extract_markdown_content(self, data: Dict[str, Any]) -> List[Tuple[str, str]]:
        """
        Extract markdown content from BDA output structure.
        Prioritizes page-level markdown to avoid duplication.
        
        Args:
            data: Parsed JSON data from BDA output
        
        Returns:
            List of tuples containing (context/section_name, markdown_text)
        """
        content = []
        
        # First, try to extract from pages (preferred method - no duplication)
        if 'pages' in data and data['pages']:
            for i, page in enumerate(data['pages']):
                if 'representation' in page and 'markdown' in page['representation']:
                    markdown_text = page['representation']['markdown'].strip()
                    if markdown_text:
                        page_num = page.get('page_index', i) + 1
                        content.append((f"Page {page_num}", markdown_text))
                        logger.info(f"Extracted markdown from page {page_num}")
            
            # If we successfully extracted from pages, return that (avoid duplication)
            if content:
                logger.info(f"Total content sections extracted from pages: {len(content)}")
                return content
        
        # Fallback: Extract from elements only if no page-level markdown found
        # This is rare but handles edge cases
        if 'elements' in data:
            logger.info("No page-level markdown found, extracting from elements")
            for element in data['elements']:
                if 'representation' in element and 'markdown' in element['representation']:
                    markdown_text = element['representation']['markdown'].strip()
                    # Skip very short elements that might be noise
                    if markdown_text and len(markdown_text) > 10:
                        element_type = element.get('sub_type', element.get('type', 'Unknown'))
                        content.append((f"{element_type} Element", markdown_text))
        
        logger.info(f"Total content sections extracted: {len(content)}")
        return content
    
    def extract_bounding_boxes(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract bounding box information from BDA elements.
        
        Args:
            data: Parsed JSON data from BDA output
        
        Returns:
            List of dictionaries containing bounding box information
        """
        boxes = []
        
        if 'elements' in data:
            for element in data['elements']:
                if 'locations' in element:
                    for location in element['locations']:
                        if 'bounding_box' in location:
                            box_entry = {
                                'element_id': element.get('id'),
                                'element_type': element.get('type'),
                                'element_sub_type': element.get('sub_type'),
                                'page_index': location.get('page_index'),
                                'bounding_box': location['bounding_box'],
                                'reading_order': element.get('reading_order')
                            }
                            
                            # Add associated text if available
                            if 'representation' in element and 'markdown' in element['representation']:
                                box_entry['associated_text'] = element['representation']['markdown']
                            
                            boxes.append(box_entry)
        
        return boxes
    
    def get_document_metadata(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract document metadata from BDA output.
        
        Args:
            data: Parsed JSON data from BDA output
        
        Returns:
            Dictionary containing document metadata
        """
        metadata = {
            'total_pages': len(data.get('pages', [])),
            'total_elements': len(data.get('elements', [])),
            'document_metadata': data.get('metadata', {})
        }
        return metadata