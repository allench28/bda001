TEAM_BRAND_MAPPING = {
    "Data Center": ["Cisco_DC", "Huawei_DP", "Huawei_DC", "IBM_HW", "Legrand", "Dell", "HPE", "Lenovo", "xFusion", "SuperMicro", "APC", "Nutanix", "Nvidia Network", "Netapp"],
    "Software Team": ["SUSE", "Rubrik", "VMware", "Veeam", "Red Hat", "Microsoft", "Commvault", "IBM instana", "IBM Watsonx", "IBM", "Omnissa"],
    "EN & Collabs": ["Cisco", "Cisco_Collab", "Huawei", "Huawei_Collab", "Juniper", "Velocloud", "Raisecom_Switch", "Zoom", "Zebra"],
    "Security Team": ["Cisco_SEC", "Cisco_S&R", "Juniper_SEC", "Juniper_S&R", "Juniper_DC", "Huawei_SEC", "Huawei_S&R", "F5", "Palo Alto Network", "TrendMicro", "Splunk", "Microsoft_SEC", "Broadcom_SEC", "iBoss", "Platform and suites", "Network Security", "Device Security", "User Security", "Cloud Security", "Application Security", "Analytics", "Industrial Security", "Security Solutions", "Data Center Security", "Secure access service edge (SASE)", "Security service edge (SSE)"]
}

DATA_CENTER_BRAND_MAPPING = {
    "Cisco_DC": ["UCS", "Nexus", "MDS", "InterSight"],
    "Huawei_DP": ["FusionModule", "FusionPower", "FusionCol", "NetECO"],
    "Huawei_DC": ["Oceanstor", "DCS"],
    "IBM_HW": ["Power", "LinuxOne", "Flashsystem"],
    "Legrand": ["Legrand"],
    "Dell": ["DELL"],
    "HPE": ["HPE"],
    "Lenovo": ["Lenovo"],
    "xFusion": ["xFusion"],
    "SuperMicro": ["SuperMicro"],
    "APC": ["APC"],
    "Nutanix": ["Nutanix"],
    "Nvidia Network": ["Nvidia Network"],
    "Netapp": ["NetApp"]
}

PERICOMP_DATA_CENTER = {
    "Cisco": ["Normal server", "AI server (GPU)", "Edge server", "SAN switch"],
    "Fujitsu": ["Flash storage", "Hybrid storage", "Normal server", "AI server (GPU)", "SAN switch", "Tape Library", "Uninterrupted Power Supply", "Rack", "PDU"],
    "Huawei": ["Flash storage", "Hybrid storage", "HPC/AI storage", "UPS", "Rack", "Cooling", "Containment system", "PDU"],
    "Lenovo": ["Normal server", "AI server (GPU)", "Edge server", "Flash storage", "Hybrid storage", "HPC/AI storage", "Tape Library", "SAN switch", "UPS", "Rack", "PDU"],
    "Legrand": ["UPS", "Structure cabling", "Rack", "Containment system", "PDU", "Cooling"],
    "Mellanox": ["Infiniband Network"],
    "Netapp": ["Flash storage", "Hybrid storage", "HPC/AI storage", "SAN switch"],
    "Pure Storage": ["Flash storage", "HPC/AI storage"],
    "Supermicro": ["Normal server", "AI server (GPU)", "Edge server"],
    "xFusion": ["Normal server", "AI server (GPU)"]
}

ASTAR_DATA_CENTER = {
    "Dell": ["Normal server", "AI server (GPU)", "SAN switch", "Hybrid storage", "HPC/AI storage", "Flash storage", "Tape Library", "Uninterrupted Power Supply", "Rack", "PDU"],
    "HPE": ["Normal server", "AI server (GPU)", "SAN switch", "Hybrid storage", "HPC/AI storage", "Flash storage", "Tape Library", "Uninterrupted Power Supply", "Rack", "PDU", "Infiniband Network"],
    "Nutanix": ["Normal server", "AI server (GPU)", "Flash storage", "Hybrid storage", "HPC/AI storage"]
}

SOFTWARE_BRAND_MAPPING = {
    "SUSE": ["SUSE"],
    "Rubrik": ["Rubrik"],
    "VMware": ["VMware"],
    "Veeam": ["Veeam"],
    "Red Hat": ["Red Hat"],
    "Microsoft": ["Microsoft"],
    "Commvault": ["Commvault"],
    "IBM instana": ["IBM instana"],
    "IBM Watsonx": ["IBM Watsonx"],
    "IBM": ["IBM"],
    "Omnissa": ["Omnissa"]
}

PERICOMP_SOFTWARE = {
    "Commvault": ["Traditional backup", "Container backup", "Replication", "Disaster recovery"],
    "F5": ["Container service", "CDN", "Cloud Native Platform"],
    "IBM": ["Database", "Container Service", "Artificial Intelligence", "Automation Middleware", "Asset Management", "Analytics", "Application Performance Monitoring"],
    "iBoss": ["Middleware"],
    "Puppet": ["Automation"],
    "Redhat": ["Operating System", "Virtualization", "Container service", "Artificial Intelligence", "Automation", "Middleware"],
    "Rubrik": ["Traditional backup", "Container backup", "Replication", "Disaster recovery"],
    "Splunk": ["Application Performance Monitoring", "Cloud Native Platform"],
    "Suse": ["Operating system", "Virtualization", "Container service", "Artificial Intelligence", "Cloud Native Platform"],
    "Tmaxsoft": ["Database"],
    "Veeam": ["Traditional Backup", "Container Backup", "Replication", "Disaster Recovery", "Cloud Native Platform"],
    "Vmware": ["Operating system", "Virtualization", "Replication", "Disaster Recovery", "Artificial Intelligence", "Automation"]
}

ASTAR_SOFTWARE = {
    "HPE": ["Virtualization"],
    "Nutanix": ["Virtualization", "Container Service", "Replication", "Disaster Recovery"],
    "Microsoft": ["Software", "Operating System", "Database", "Virtualization", "Container Service", "Replication", "Artificial Intelligence", "Automation", "Analytics", "Cloud Native Platform"]
}

EN_N_COLLABS_BRAND_MAPPING = {
    "Cisco": ["Cisco_Router & Switch & Wireless", "Catalyst", "Meraki"],
    "Cisco_Collab": ["PABX", "Webex", "A-Flex", "VideoConference"],
    "Huawei": ["Huawei_Router & Switch & Wireless", "CloudEngine", "AirEngine"],
    "Huawei_Collab": ["Ideahub"],
    "Juniper": ["Juniper_Router & CampusSwitch & DataCenterSwitch & Wireless"],
    "Velocloud": ["VMware_SD-WAN"],
    "Raisecom_Switch": ["Raisecom_Switch"],
    "Zoom": ["Zoom"],
    "Zebra": ["Handheld", "Printer"],
}

PERICOMP_EN_COLLABS = {
    "Cisco": ["Router", "Access Point", "Switches", "SD WAN", "IP Phone", "Room Bar", "Codec", "Webex", "Ideahub"],
    "Huawei": ["Router", "Access Point", "Switches", "SD WAN", "GPON", "Ideahub"],
    "Juniper": ["Router", "Access Point", "Switches", "SD WAN"],
    "Vmware": ["Router", "SD WAN"],
    "Raisecomm": ["Router", "Switches", "GPON"],
    "Zoom": ["Virtual Meeting"]
}

ASTAR_EN_COLLABS = {
    "Zebra": ["Printer", "Handheld"]
}

SECURITY_BRAND_MAPPING = {
    "Cisco_SEC": ["Firepower", "ASA", "FMC", "Cisco ISE", "Secure Portfolio", "Meraki MX", "ISA"],
    "Cisco_S&R": ["Catalyst C9K", "Catalyst 8K", "Meraki MS", "Meraki MV", "Meraki MX", "Meraki MDM", "IE Series", "IR series"],
    "Juniper_SEC": ["SRX series", "Security Director", "Secure Connect", "Secure Edge Access"],
    "Juniper_S&R": ["EX series", "QFX series", "SRX series", "MX series", "ACX Series", "SSR series", "Mist", "Junos Space"],
    "Juniper_DC": ["QFX series", "PTX series", "Apstra"],
    "Huawei_SEC": ["HiSecEngine/USG series", "SecoManager", "AntiDDOS series"],
    "Huawei_S&R": ["CloudEngine/S series", "AR series", "NetEngine"],
    "F5": ["F5"],
    "Palo Alto Network": ["Palo Alto Network"],
    "TrendMicro": ["TrendMicro"],
    "Splunk": ["Splunk"],
    "Microsoft_SEC": ["Microsoft Defender"],
    "Broadcom_SEC": ["AVI network (NSX)"],
    "iBoss": ["iBoss"]
}

CISCO_SECURE_PORTFOLIO_BRAND_MAPPING = {
    "Platform and suites": [
        "Cisco Security Cloud", "Cisco breach Protection", "Cisco Cloud Protection", "Cisco User Protection"
    ],
    "Network Security": [
        "Cisco Firewall (Firepower & ASA)", "Cisco Security Cloud Control", "Cisco Identity Services Engine (ISE)",
        "Cisco Multicloud Defense", "Cisco XDR"
    ],
    "Device Security": [
        "Cisco Secure Client (Anyconnect)", "Cisco Secure Endpoint", "Cisco Security Connector",
        "Cisco Meraki Systems Managers (SM)"
    ],
    "User Security": [
        "Cisco DUO", "Cisco Secure Email Threat Defense", "Cisco Secure Access", "Cisco Secure Web Appliance"
    ],
    "Cloud Security": [
        "Cisco AI Defense", "Cisco Attack Surface Management", "Cisco Umbrella"
    ],
    "Application Security": [
        "Cisco Hypershield", "Cisco Secure Workload", "Cisco Web Application & API Protection (WAAP)"
    ],
    "Analytics": [
        "Cisco Secure Malware Analytics", "Cisco Secure Network Analytics", "Cisco Security Analytics and Logging",
        "Cisco Telemetry Broker"
    ],
    "Industrial Security": [
        "Cisco Industrial Threat Defense", "Cisco Cyber Vision", "Cisco Secure Equipment Access"
    ],
    "Security Solutions": [
        "Cisco Identity Intelligence", "Cisco Secure Hybrid Work"
    ],
    "Data Center Security": ["Industrial cybersecurity"],
    "Secure access service edge (SASE)": ["Security AI"],
    "Security service edge (SSE)": ["Zero Trust Access", "Zero trust"]
}

PERICOMP_SECURITY = {
    "Cisco": [
        "Remote Access VPN", "Endpoint Security", "Network Security", "Workload Security", "DNS Security",
        "Cloud Access Service Broker", "Workload Segmentation", "Network Access Control", "Email Security",
        "Next Generation Firewall", "Sandboxing (Malware Protection)", "Intrusion Prevention / Detection System",
        "SSO", "MFA", "DDOS (L3 & L4)", "DDOS (DNS)", "XDR", "SASE (SD-WAN + SSE)", "SOAR",
        "Identity & Access Management", "User behavioral Analytic"
    ],
    "F5": [
        "DNS Security", "Next Generation Firewall", "Intrusion Prevention / Detection System", "SSO", "MFA",
        "ADC (LB & SLB)", "WAF", "API Security (Discovery & protection)", "API Gateway", "DDOS (L3 & L4)",
        "DDOS L7", "DDOS (DNS)", "Kubernetes Ingress Controller", "Bot Defense", "AI Gateway"
    ],
    "Huawei": [
        "Remote Access VPN", "Network Security", "Network Access Control", "Next Generation Firewall",
        "Sandboxing (Malware Protection)", "Intrusion Prevention / Detection System", "DDOS (L3 & L4)"
    ],
    "IBM": [
        "Database Security", "DDOS (L3 & L4)", "SIEM Identity & Access Management", "User behavioral Analytic"
    ],
    "Juniper": [
        "DNS Security", "Next Generation Firewall", "Intrusion Prevention / Detection System", "SSO", "MFA",
        "ADC (LB & SLB)", "WAF", "API Security (Discovery & protection)", "API Gateway", "DDOS (L3 & L4)",
        "DDOS L7", "DDOS (DNS)", "Kubernetes Ingress Controller", "Bot Defense", "AI Gateway",
        "SASE (SD-WAN + SSE)", "SOAR"
    ],
    "Palo Alto Networks": [
        "Remote Access VPN", "Endpoint Security", "Network Security", "Workload Security", "DNS Security",
        "Cloud Access Service Broker", "Attack Surface Risk Management", "Next Generation Firewall",
        "Sandboxing (Malware Protection)", "Intrusion Prevention / Detection System", "DDOS (L3 & L4)",
        "DDOS (DNS)", "XDR", "SASE (SD-WAN + SSE)", "SIEM", "SOAR"
    ],
    "Splunk": [
        "SIEM", "SOAR", "User behavioral Analytic", "Observability"
    ],
    "Suse": [
        "Observability"
    ],
    "Trend Micro": [
        "Endpoint Security", "Network Security", "Workload Security", "Cloud Access Service Broker",
        "Email Security", "Attack Surface Risk Management", "Sandboxing (Malware Protection)",
        "Intrusion Prevention / Detection System", "DDOS (L3 & L4)", "DDOS (DNS)", "XDR",
        "SASE (SD-WAN + SSE)", "SIEM", "SOAR"
    ],
    "Vmware": [
        "Network Security", "Next Generation Firewall", "Sandboxing (Malware Protection)", "ADC (LB & SLB)",
        "WAF", "API Gateway", "Kubernetes Ingress Controller", "Identity & Access Management", "Observability"
    ]
}

ASTAR_SECURITY = {
    "Microsoft": ["Email Security", "Intrusion Prevention / Detection System", "SSO", "MFA", "XDR", "Identity & Access Management"],
    "Kaspersky": ["Endpoint Security", "XDR", "SIEM"]
}
