FA_CLASS_MAPPING_PROMPT = """
TASK:
Map the input posting group to the correct FA Class Code from the database.
"FaClassCode" and "FaClassName" is used to categorize fixed assets for accounting purposes.

FA CLASS MAPPING RULES:
- With the "postingGroup" in input data, map to"faClassCode" to determine the most appropriate FA Class Code.
- Exact match on "postingGroup" with faClassCode is highest priority.
- You can use "description" from input to help identify the correct class code, referring to the "faClassCodeCode" and "faClassCodeName" in the database. (Secondary priority)
- If no suitable match is found, default to the provided postingGroup. (Lowest priority)

IMPORTANT:
- You are to return one of the faClassCode from the database only.

MAPPING SCENARIOS:
1. If you can match postingGroup:
   a. Set faClassCode to the code from the database
   b. Set completeMapping to true
   c. Set confidence to a float between 0.9 and 1.0

2. If no match at all:
   a. Set faClassCode to the input postingGroup
   b. Set completeMapping to false
   c. Set confidence to 0.0
   d. Set exception to "No match found for postingGroup"

Mapping Example:
Input: 
1. Posting Group: S-COM, Description: OFFICE COMPUTER
2. Posting Group: MV, Description: DELIVERY VAN
3. Posting Group: OE, Description: OFFICE CHAIR
4. Posting Group: P-EQUIP, Description: PACKAGING MACHINE
5. Posting Group: RE, Description: WAREHOUSE RACKING
6. Posting Group: IT, Description: NETWORK SWITCH

Database:
"faClassCode": "S-COM", "faClassCodeName": "STORE COMPUTER",
"faClassCode": "MV", "faClassCodeName": "MOTOR VEHICLE", 
"faClassCode": "OE", "faClassCodeName": "OFFICE EQUIPMENT", 
"faClassCode": "P-EQUIP", "faClassCodeName": "PUDO EQUIPMENT", 
"faClassCode": "RE", "faClassCodeName": "RENOVATION",
"faClassCode": "IT", "faClassCodeName": "COMPUTER"

Output:
1. faClassCode: S-COM, completeMapping: true, confidence: 0.95
2. faClassCode: MV, completeMapping: true, confidence: 0.95
3. faClassCode: OE, completeMapping: true, confidence: 0.95
4. faClassCode: P-EQUIP, completeMapping: true, confidence: 0.95
5. faClassCode: RE, completeMapping: true, confidence: 0.95
6. faClassCode: IT, completeMapping: true, confidence: 0.95

DATABASE:
<database>
{database}
</database>

INPUT:
<input>
{input_item}
</input>

OUTPUT FORMAT:
Return ONLY a valid JSON object without any additional text, explanations, markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
Ensure the following keys are present:
{{
    "faClassCode": "S-EQUIP",
    "completeMapping": true/false,
    "confidence": 0.95,
    "exception": Appropriate error message if any issues arise (e.g., "No match found for postingGroup")
}}
"""

DEPRECIATION_RATE_MAPPING_PROMPT = """
TASK:
Map the input FA Class Code, FA Class Name, and description to the correct depreciation rate from the database.
The depreciation table uses different category codes than FA Class codes, so you need to intelligently map between them.

DEPRECIATION RATE MAPPING RULES:
- Primary: Direct match of "faClassCode" with "depreciationCategory" in the database.
- Secondary: If no direct match, use "faClassName" to identify the correct depreciation category.
- Tertiary: Use "description" to help identify the asset type and find the most appropriate depreciation rate.
- If no suitable match is found, default the rate to 0.0%.

IMPORTANT MAPPING CONSIDERATIONS:
- FA Class codes may differ from depreciation categories. Examples:
  * FA Class "OE" (Office Equipment) → Depreciation Category "OFFEQUIP" 
  * FA Class "S-COM" (Store Computer) → Depreciation Category "S-COMPUTER"
  * FA Class "MV" (Motor Vehicle) → May not exist in depreciation table, use closest match
- Look for semantic similarity between FA Class names and depreciation categories
- Consider the asset type described in the description field

MAPPING SCENARIOS:
1. Direct match found (faClassCode matches depreciationCategory):
   a. Set depreciationRate to the rate from the database (remove '%' symbol, convert to float)
   b. Set completeMapping to true
   c. Set confidence to a float between 0.9 and 1.0
   d. Set mappedCategory to the matched depreciationCategory

2. Semantic match found (faClassName matches category type):
   a. Set depreciationRate to the closest matching rate
   b. Set completeMapping to true
   c. Set confidence to a float between 0.7 and 0.89
   d. Set mappedCategory to the matched depreciationCategory

3. Partial match (description-based matching):
   a. Set depreciationRate to best estimate based on description
   b. Set completeMapping to false
   c. Set confidence between 0.5 and 0.69
   d. Set mappedCategory to the matched depreciationCategory

4. No match found:
   a. Set depreciationRate to 0.0
   b. Set completeMapping to false
   c. Set confidence to 0.0
   d. Set mappedCategory to "DEFAULT"
   e. Set exception to "No suitable match found for FA Class"

MAPPING EXAMPLES:
Input Examples:
1. faClassCode: "OE", faClassName: "OFFICE EQUIPMENT", description: "DESK CHAIR"
2. faClassCode: "S-COM", faClassName: "STORE COMPUTER", description: "POINT OF SALE SYSTEM"
3. faClassCode: "MV", faClassName: "MOTOR VEHICLE", description: "DELIVERY VAN"
4. faClassCode: "S-EQUIP", faClassName: "STORE EQUIPMENT", description: "CASH REGISTER"
5. faClassCode: "RE", faClassName: "RENOVATION", description: "STORE FLOORING"

Example Database:
1. depreciationCategory: "OFFEQUIP", capexCategory: "Office equipment", rate: "10%"
2. depreciationCategory: "S-COMPUTER", capexCategory: "Computer equipment", rate: "20%"
3. depreciationCategory: "MV", capexCategory: "Motor vehicle", rate: "15%"
4. depreciationCategory: "S-EQUIP", capexCategory: "Tools & Equipment", rate: "10%"
5. depreciationCategory: "RENOVATION", capexCategory: "Renovation", rate: "10%"

Expected Output Examples:
1. depreciationRate: 10.0, mappedCategory: "OFFEQUIP", completeMapping: true, confidence: 0.85
2. depreciationRate: 20.0, mappedCategory: "S-COMPUTER", completeMapping: true, confidence: 0.90
3. depreciationRate: 10.0, mappedCategory: "DEFAULT", completeMapping: false, confidence: 0.0
4. depreciationRate: 10.0, mappedCategory: "S-EQUIP", completeMapping: true, confidence: 0.95
5. depreciationRate: 10.0, mappedCategory: "RENOVATION", completeMapping: true, confidence: 0.90

DATABASE:
<database>
{database}
</database>

INPUT:
<input>
{input_item}
</input>

OUTPUT FORMAT:
Return ONLY a valid JSON object without any additional text, explanations, markdown formatting (like ```json), code blocks, or any other content. Do not include backticks or any formatting characters. The JSON must be directly parseable by json.loads().
Ensure the following keys are present:
{{
    "depreciationRate": 10.0,
    "mappedCategory": "S-EQUIP",
    "completeMapping": true,
    "confidence": 0.95,
    "exception": "Error message if any issues arise (optional)"
}}
"""