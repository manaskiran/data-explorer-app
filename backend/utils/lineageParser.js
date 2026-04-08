const DAG_LAYER_PATTERNS = [
  { pattern: /^get_(csv|yaml|json)_/i,                       layer: 'source',   transformation: 'extraction' },
  { pattern: /^put_(csv|yaml)_source_to_staging_|^push_(csv|yaml)_staging_(?!to)/i,  layer: 'staging',  transformation: 'staging_push' },
  { pattern: /^push_(csv|yaml)_staging_to_hdfs_/i,                   layer: 'raw_hdfs',  transformation: 'hdfs_push' },
  { pattern: /^ingest_(csv_)?hdfs_to_hudi_/i,                        layer: 'raw_hudi',  transformation: 'hudi_ingestion' },
  { pattern: /^create_(techsophy_|medunited_)?.*_curate[d]?$/i,       layer: 'curated',   transformation: 'curation' },
  { pattern: /^create_(star_schema_|table_.*_service|.*_service(_delta)?$)/i,        layer: 'service',   transformation: 'service_load' },
  { pattern: /^(upload_|create_(daily|monthly)_)/i,                  layer: 'reporting', transformation: 'reporting_upload' },
  { pattern: /^master_/i,                                            layer: 'orchestrator', transformation: 'orchestration' },
];

const LAYER_ORDER = ['source', 'staging', 'raw_hdfs', 'raw_hudi', 'curated', 'service', 'reporting'];

const extractPipelineKey = (dagId) => {
  let s = dagId.toLowerCase();
  s = s.replace(/^(get|put|push|ingest|create|upload|master)_/, '');
  s = s.replace(/^(csv|yaml|json)_/, '');
  s = s.replace(/^source_to_staging_/, '').replace(/^staging_to_hdfs_/, '').replace(/^hdfs_to_hudi_/, '').replace(/^staging_/, '');
  s = s.replace(/^(techsophy|medunited)_/, '');
  s = s.replace(/_curated?$/, '').replace(/_service(_delta)?$/, '').replace(/_hdfs$/, '').replace(/_hudi$/, '').replace(/_staging$/, '');
  return s.trim() || dagId;
};

const extractApplication = (dagId) => {
  const key = extractPipelineKey(dagId);
  const parts = key.split('_');
  const genericSuffixes = ['postgres', 'postgresql', 'mongodb', 'mongo', 'mysql', 'mssql', 'oracle', 'archive', 'raw', 'dump'];
  if (parts.length > 1 && genericSuffixes.includes(parts[parts.length - 1])) {
    return parts.slice(0, -1).join('_');
  }
  return key;
};

const detectLayer = (dagId) => {
  for (const { pattern, layer, transformation } of DAG_LAYER_PATTERNS) {
    if (pattern.test(dagId)) return { layer, transformation };
  }
  return { layer: 'unknown', transformation: 'unknown' };
};

const LAYER_LABELS = {
  source:    { label: 'Source',    color: '#3b82f6' },
  staging:   { label: 'Staging',   color: '#8b5cf6' },
  raw_hdfs:  { label: 'Raw HDFS',  color: '#0ea5e9' },
  raw_hudi:  { label: 'Raw Hudi',  color: '#06b6d4' },
  curated:   { label: 'Curated',   color: '#10b981' },
  service:   { label: 'Service',   color: '#f59e0b' },
  reporting: { label: 'Reporting', color: '#ef4444' },
};

module.exports = { detectLayer, extractApplication, LAYER_LABELS, LAYER_ORDER };
