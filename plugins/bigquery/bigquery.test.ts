import { handler as bigQueryHandler } from './bigquery';
import { BigQuery } from '@google-cloud/bigquery';

jest.mock('@google-cloud/bigquery');

const mockInsert = jest.fn();
const mockTable = jest.fn(() => ({ insert: mockInsert }));
const mockDataset = jest.fn(() => ({ table: mockTable }));

(BigQuery as jest.Mock).mockImplementation(() => ({
  dataset: mockDataset,
}));

describe('BigQuery handler', () => {
  const eventType = 'endHook';
  const context = {
    request: {
      id: 'test-id',
      model: 'gpt-3.5-turbo',
      prompt: 'Test prompt',
    },
    response: {
      text: 'Test response',
      status: 200,
    },
  };
  const parameters = {
    credentials: {
      projectId: 'test-project',
      clientEmail: 'test@example.com',
      privateKey: 'test-private-key',
    },
    datasetId: 'test-dataset',
    tableId: 'test-table',
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('should log data to BigQuery successfully', async () => {
    mockInsert.mockResolvedValue([]);

    const result = await bigQueryHandler(context, parameters, eventType);

    expect(result.verdict).toBe(true);
    expect(result.error).toBeNull();
    expect(result.data).toEqual({ message: 'Successfully logged to BigQuery' });
    expect(BigQuery).toHaveBeenCalledWith({
      projectId: 'test-project',
      credentials: {
        client_email: 'test@example.com',
        private_key: 'test-private-key',
      },
    });
    expect(mockDataset).toHaveBeenCalledWith('test-dataset');
    expect(mockTable).toHaveBeenCalledWith('test-table');
    expect(mockInsert).toHaveBeenCalled();
  });

  it('should handle errors when logging to BigQuery fails', async () => {
    const testError = new Error('BigQuery insert failed');
    mockInsert.mockRejectedValue(testError);

    const result = await bigQueryHandler(context, parameters, eventType);

    expect(result.verdict).toBe(false);
    expect(result.error).toBe(testError);
    expect(result.data).toEqual({
      message: 'Failed to log to BigQuery',
      error: 'BigQuery insert failed',
    });
  });
});
