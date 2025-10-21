import requests
from requests_oauthlib import OAuth1
import hmac
import hashlib
import base64
from custom_exceptions import ResourceNotFoundException, BadRequestException
class NetSuiteClient:
    """
    NetSuite Client using SuiteQL API
    
    Handles authentication and API requests to NetSuite using SuiteQL for efficient data retrieval.
    """
    
    def __init__(self, account, consumer_key, consumer_secret, token_id, token_secret):
        """Initialize the NetSuite client"""
        self.account = account
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.token_id = token_id
        self.token_secret = token_secret
        
        # NetSuite API base URL
        self.base_url = f"https://{self.account}.suitetalk.api.netsuite.com/services/rest"

        # Initialize OAuth configuration
        self.oauth = self._get_oauth_config()
    
    def _hash_function(self, base_string, key):
        """
        Creates a SHA-256 HMAC hash of the base string using the given key.
        """
        return base64.b64encode(
            hmac.new(key.encode(), base_string.encode(), hashlib.sha256).digest()
        ).decode()
    
    def _get_oauth_config(self):
        """
        Initialize and return an OAuth1 config
        """
        return OAuth1(
            client_key=self.consumer_key,
            client_secret=self.consumer_secret,
            resource_owner_key=self.token_id,
            resource_owner_secret=self.token_secret,
            signature_method='HMAC-SHA256',
            signature_type='auth_header',
            realm=self.account.upper()
        )
    
    def _make_request(self, method, endpoint, data=None, params=None):
        """
        Make an authenticated request to the NetSuite API
        """
        url = f"{self.base_url}{endpoint}"

        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json' if data else None,
            'Prefer': 'transient'  # Better performance for read operations
        }
        
        response = requests.request(
            method=method,
            url=url,
            auth=self.oauth,
            headers={k: v for k, v in headers.items() if v is not None},
            params=params,
            json=data
        )
        
        # Raise exception for non-2xx responses
        response.raise_for_status()
        
        # Return JSON response if present
        if response.text:
            return response.json()
        
        return {}

    def execute_suiteql(self, query, params=None):
        """
        Execute a SuiteQL query
        
        Args:
            query: SuiteQL query string
            params: Query parameters dictionary
            
        Returns:
            list: Query results
        """
        
        endpoint = "/query/v1/suiteql"

        # Process query parameters
        if params:
            for key, value in params.items():
                placeholder = f":{key}"
                if isinstance(value, str):
                    # If value is string, wrap in single quotes
                    value = f"'{value}'"
                elif value is None:
                    value = 'NULL'
                query = query.replace(placeholder, str(value))
        
        data = {"q": query}
        
        response = self._make_request('POST', endpoint, data=data)

        if not response:
            raise BadRequestException("No response from NetSuite API")
        
        records = response.get('items')

        if not records:
            raise ResourceNotFoundException("No records found for the given query")
        
        return records
    
    def get_purchase_orders(self):
        """
        Get purchase orders from NetSuite using SuiteQL
        
        Returns:
            list: Two-item list where the first item is header data and second item is line items data
        """
        
        header_query = """
            SELECT 
                t.id,
                t.tranDisplayName, 
                TO_CHAR(t.trandate, 'YYYY-MM-DD') as trandate, 
                t.tranid, 
                TO_CHAR(t.duedate, 'YYYY-MM-DD') as duedate,
                te.name as terms, 
                v.companyName vendorName,
                v.id vendorId,
                ab.addrtext vendorAddress,
                c.name currency,  
                ts.addrtext as shippingaddress,
                TO_CHAR(t.shipdate, 'YYYY-MM-DD') as shipdate,
                tstat.name as status,
            FROM transaction t 
            LEFT JOIN vendor v ON t.entity = v.id 
            LEFT JOIN vendoraddressbook vab ON v.id = vab.entity
            LEFT JOIN vendorAddressbookEntityAddress ab ON vab.addressbookaddress = ab.nkey
            LEFT JOIN term te ON t.terms = te.id
            LEFT JOIN currency c ON t.currency = c.id 
            LEFT JOIN transactionShippingAddress ts ON t.shippingaddress = ts.nkey
            LEFT JOIN approvalstatus aps ON t.approvalStatus = aps.id
            LEFT JOIN transactionstatus tstat ON t.status = tstat.id AND t.type = tstat.trantype
            WHERE t.type = 'PurchOrd'
        """
        
        try:
            purchase_orders = self.execute_suiteql(header_query)

            print(f"Purchase Orders Count: {len(purchase_orders)}")
        except Exception as e:
            return None
        
        po_ids = [str(po.get('id')) for po in purchase_orders if po.get('id')]
        po_ids_str = ", ".join(po_ids)
        
        items_query = f"""
            SELECT 
                tl.transaction as po_id,
                i.name, 
                i.id as itemid,
                tl.memo, 
                tl.quantity, 
                tl.netAmount,
                tl.taxLine,
                tl.mainLine,
                tl.rate,
                uom.abbreviation, 
                uom.unitname,
                s.fullname as buyername,
                s.id as buyercode,
                sad.addrtext as buyeraddress 
            FROM transactionline tl
            LEFT JOIN generalizeditem i ON tl.item = i.id 
            LEFT JOIN unitstypeuom uom ON tl.units = uom.internalid
            LEFT JOIN subsidiary s ON tl.subsidiary = s.id
            LEFT JOIN subsidiarymainaddress sad ON s.mainaddress = sad.nkey 
            WHERE tl.mainline = 'F' AND tl.transaction IN ({po_ids_str})
        """
        
        try:
            all_line_items = self.execute_suiteql(items_query)

        except Exception as e:
            return None
        
        tax_lines = [line for line in all_line_items if line.get('taxline') == 'T']

        # Group tax lines by purchase order ID for efficient lookup
        tax_lines_by_po = {}
        for tax_line in tax_lines:
            po_id = tax_line.get('po_id')
            if po_id:
                if po_id not in tax_lines_by_po:
                    tax_lines_by_po[po_id] = []
                tax_lines_by_po[po_id].append(tax_line)

        # Associate tax information with each purchase order
        for po in purchase_orders:
            # Add buyer information to purchase order
            po['buyername'] = all_line_items[0].get('buyername','-') if len(all_line_items) > 0 else '-'
            po['buyercode'] = all_line_items[0].get('buyercode','-') if len(all_line_items) > 0 else '-'
            po['buyeraddress'] = all_line_items[0].get('buyeraddress','-') if len(all_line_items) > 0 else '-'
            
            # Initialize tax fields
            po['taxcode'] = None
            po['taxrate'] = None
            po['taxtype'] = None

            po_id = po.get('id')
            if po_id and po_id in tax_lines_by_po:
                po_tax_lines = tax_lines_by_po[po_id]
                # If multiple tax lines exist, we could combine them or take the first one
                if po_tax_lines:
                    po['taxcode'] = po_tax_lines[0].get('name')
                    po['taxrate'] = po_tax_lines[0].get('rate')
                    po['taxtype'] = po_tax_lines[0].get('memo')
                    po['taxamount'] = po_tax_lines[0].get('netamount', 0)

            item_lines = [line for line in all_line_items if line.get('taxline') == 'F']

            # Add total tax amount to each item lin
            totalAmount = sum(float(line.get('rate', 0)) * float(line.get('quantity',1)) for line in item_lines if line.get('po_id') == po_id)
            totalTaxAmount = sum(float(line.get('taxamount', 0)) for line in item_lines if line.get('po_id') == po_id)

            # Add total amounts to purchase order
            po['totalAmountWithTax'] = str(
                totalAmount + totalTaxAmount
            )
            po['totalAmountWithoutTax'] = str(totalAmount)
            po['totalTaxAmount'] = str(totalTaxAmount)

            for item in item_lines:
                # add per item tax amount by multiplying rate with netAmount
                item['taxamount'] = str(float(po.get('taxrate', 0)) * 0.01 * float(item.get('netamount', 0))) 
 
        
        return [purchase_orders, item_lines]

    def get_item_receipts(self):
        """
        Get item receipts from NetSuite using SuiteQL
        
        Returns:
            list: Two-item list where the first item is header data and second item is line items data
        """
        
        header_query = """
            SELECT 
            t.id,
            t.tranId,
            t.tranDisplayName,
            TO_CHAR(t.trandate, 'YYYY-MM-DD') as trandate,
            v.companyName AS vendorName,
            v.id AS vendorId,
            ab.addrtext AS vendorAddress,
            c.name AS currency,
            trl.createdfrom AS po_id,
            po.tranid AS po_number,
            tstat.name as status,
        FROM 
            transaction t
        JOIN 
            transactionline trl ON t.id = trl.transaction
        LEFT JOIN 
            transaction po ON trl.createdfrom = po.id
        LEFT JOIN 
            vendor v ON t.entity = v.id
        LEFT JOIN 
            vendoraddressbook vab ON v.id = vab.entity
        LEFT JOIN 
            vendorAddressbookEntityAddress ab ON vab.addressbookaddress = ab.nkey
        LEFT JOIN 
            currency c ON t.currency = c.id
        LEFT JOIN 
            transactionstatus tstat ON t.status = tstat.id AND t.type = tstat.trantype
        WHERE 
            t.type = 'ItemRcpt'
            AND trl.mainline = 'T'
            AND trl.createdfrom IS NOT NULL
        """
        
        try:
            item_receipts = self.execute_suiteql(header_query)

            print(f"Item Receipts Count: {len(item_receipts)}")
        except Exception as e:
            return None
        
        ir_ids = [str(ir.get('id')) for ir in item_receipts if ir.get('id')]
        ir_ids_str = ", ".join(ir_ids)
        items_query = f"""
            SELECT 
                tl.transaction as ir_id,
                i.name, 
                i.id as itemid,
                tl.memo, 
                tl.quantity, 
                tl.netAmount,
                tl.taxLine,
                tl.mainLine,
                tl.rate,
                uom.abbreviation, 
                uom.unitname 
            FROM transactionline tl
            LEFT JOIN generalizeditem i ON tl.item = i.id 
            LEFT JOIN unitstypeuom uom ON tl.units = uom.internalid 
            WHERE tl.mainline = 'F' AND tl.transaction IN ({ir_ids_str})
        """
        try:
            all_line_items = self.execute_suiteql(items_query)
        except Exception as e:
            return None
        tax_lines = [line for line in all_line_items if line.get('taxline') == 'T']
        # Group tax lines by item receipt ID for efficient lookup
        tax_lines_by_ir = {}    
        for tax_line in tax_lines:
            ir_id = tax_line.get('ir_id')
            if ir_id:
                if ir_id not in tax_lines_by_ir:
                    tax_lines_by_ir[ir_id] = []
                tax_lines_by_ir[ir_id].append(tax_line)
        # Associate tax information with each item receipt
        for ir in item_receipts:
            ir['taxcode'] = None
            ir['taxrate'] = None
            ir['taxtype'] = None

            ir_id = ir.get('id')
            if ir_id and ir_id in tax_lines_by_ir:
                ir_tax_lines = tax_lines_by_ir[ir_id]
                # If multiple tax lines exist, we could combine them or take the first one
                if ir_tax_lines:
                    ir['taxcode'] = ir_tax_lines[0].get('name')
                    ir['taxrate'] = ir_tax_lines[0].get('rate')
                    ir['taxtype'] = ir_tax_lines[0].get('memo')

            item_lines = [line for line in all_line_items if line.get('taxline') == 'F']
            totalAmount = sum(float(line.get('rate', 0)) * float(line.get('quantity',1)) for line in item_lines if line.get('ir_id') == ir_id)
            ir['totalAmount'] = str(totalAmount)



        return [item_receipts, item_lines]
                
    def get_records(self, record_type):
        """
        Generic method to get records from NetSuite based on record type
        """
        get_records_mapping = {
            "purchase-order": self.get_purchase_orders,
            "item-receipt": self.get_item_receipts,
        }

        selected_method = get_records_mapping.get(record_type)

        if not selected_method:
            return None
        
        return selected_method()