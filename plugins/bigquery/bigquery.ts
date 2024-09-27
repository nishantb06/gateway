import {
  HookEventType,
  PluginContext,
  PluginHandler,
  PluginParameters,
} from '../types';
import { BigQuery } from '@google-cloud/bigquery';

interface BigQueryCredentials {
  projectId: string;
  clientEmail: string;
  privateKey: string;
}

interface BigQueryParameters extends PluginParameters {
  credentials: BigQueryCredentials;
  datasetId: string;
  tableId: string;
}

export const handler: PluginHandler = async (
  context: PluginContext,
  parameters: BigQueryParameters,
  eventType: HookEventType
) => {
  let error = null;
  let verdict = true;
  let data = null;

  try {
    const { projectId, clientEmail, privateKey } = parameters.credentials;
    const { datasetId, tableId } = parameters;

    const bigquery = new BigQuery({
      projectId,
      credentials: {
        client_email: clientEmail,
        private_key: privateKey,
      },
    });

    const dataset = bigquery.dataset(datasetId);
    const table = dataset.table(tableId);

    const row = {
      timestamp: new Date().toISOString(),
      request_id: context.request.id,
      request_model: context.request.model,
      request_prompt: context.request.prompt,
      response_text: context.response?.text || '',
      response_status: context.response?.status || '',
      // Add more fields as needed
    };

    await table.insert(row);

    data = { message: 'Successfully logged to BigQuery' };
  } catch (e: any) {
    error = e;
    verdict = false;
    data = { message: 'Failed to log to BigQuery', error: e.message };
  }

  return { error, verdict, data };
};
