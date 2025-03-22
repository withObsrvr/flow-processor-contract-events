package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/stellar/go/strkey"
	"github.com/stellar/go/toid"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
	"github.com/withObsrvr/pluginapi"
)

// ContractEvent represents an event emitted by a contract with both raw and decoded data
type ContractEvent struct {
	// Transaction context
	TransactionHash   string    `json:"transaction_hash"`
	TransactionID     int64     `json:"transaction_id"`
	Successful        bool      `json:"successful"`
	LedgerSequence    uint32    `json:"ledger_sequence"`
	ClosedAt          time.Time `json:"closed_at"`
	NetworkPassphrase string    `json:"network_passphrase"`

	// Event context
	ContractID         string `json:"contract_id"`
	EventIndex         int    `json:"event_index"`
	OperationIndex     int    `json:"operation_index"`
	InSuccessfulTxCall bool   `json:"in_successful_tx_call"`

	// Event type information
	Type     string `json:"type"`
	TypeCode int32  `json:"type_code"`

	// Event data - both raw and decoded
	Topics        []TopicData `json:"topics"`
	TopicsDecoded []TopicData `json:"topics_decoded"`
	Data          EventData   `json:"data"`
	DataDecoded   EventData   `json:"data_decoded"`

	// Raw XDR for archival and debugging
	EventXDR string `json:"event_xdr"`

	// Additional diagnostic data
	DiagnosticEvents []DiagnosticData `json:"diagnostic_events,omitempty"`

	// Metadata for querying and filtering
	Tags map[string]string `json:"tags,omitempty"`
}

// TopicData represents a structured topic with type information
type TopicData struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

// EventData represents the data payload of a contract event
type EventData struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

// DiagnosticData contains additional diagnostic information about an event
type DiagnosticData struct {
	Event                    json.RawMessage `json:"event"`
	InSuccessfulContractCall bool            `json:"in_successful_contract_call"`
	RawXDR                   string          `json:"raw_xdr,omitempty"`
}

// ContractEventProcessor handles processing of contract events from transactions
type ContractEventProcessor struct {
	networkPassphrase string
	consumers         []pluginapi.Consumer
	mu                sync.RWMutex
	stats             struct {
		ProcessedLedgers  uint32
		EventsFound       uint64
		SuccessfulEvents  uint64
		FailedEvents      uint64
		LastLedger        uint32
		LastProcessedTime time.Time
	}
}

func (p *ContractEventProcessor) Initialize(config map[string]interface{}) error {
	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return fmt.Errorf("missing network_passphrase in configuration")
	}
	p.networkPassphrase = networkPassphrase
	return nil
}

func (p *ContractEventProcessor) RegisterConsumer(consumer pluginapi.Consumer) {
	log.Printf("ContractEventProcessor: Registering consumer %s", consumer.Name())
	p.consumers = append(p.consumers, consumer)
}

func (p *ContractEventProcessor) Process(ctx context.Context, msg pluginapi.Message) error {
	ledgerCloseMeta, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		return fmt.Errorf("expected xdr.LedgerCloseMeta, got %T", msg.Payload)
	}

	sequence := ledgerCloseMeta.LedgerSequence()
	log.Printf("Processing ledger %d for contract events", sequence)

	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(p.networkPassphrase, ledgerCloseMeta)
	if err != nil {
		return fmt.Errorf("error creating transaction reader: %w", err)
	}
	defer txReader.Close()

	// Process each transaction
	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading transaction: %w", err)
		}

		// Get diagnostic events from transaction
		diagnosticEvents, err := tx.GetDiagnosticEvents()
		if err != nil {
			log.Printf("Error getting diagnostic events: %v", err)
			continue
		}

		// Process events
		for opIdx, events := range filterContractEvents(diagnosticEvents) {
			for eventIdx, event := range events {
				contractEvent, err := p.processContractEvent(tx, opIdx, eventIdx, event, ledgerCloseMeta)
				if err != nil {
					log.Printf("Error processing contract event: %v", err)
					p.mu.Lock()
					p.stats.FailedEvents++
					p.mu.Unlock()
					continue
				}

				if contractEvent != nil {
					// Add debug logging
					log.Printf("Found contract event for contract ID: %s", contractEvent.ContractID)

					p.mu.Lock()
					p.stats.EventsFound++
					if contractEvent.Successful {
						p.stats.SuccessfulEvents++
					}
					p.mu.Unlock()

					if err := p.forwardToConsumers(ctx, contractEvent); err != nil {
						log.Printf("Error forwarding event: %v", err)
					}
				}
			}
		}
	}

	// Update processor stats
	p.mu.Lock()
	p.stats.ProcessedLedgers++
	p.stats.LastLedger = sequence
	p.stats.LastProcessedTime = time.Now()
	p.mu.Unlock()

	return nil
}

func (p *ContractEventProcessor) forwardToConsumers(ctx context.Context, event *ContractEvent) error {
	// Add debug logging
	log.Printf("Forwarding event to %d consumers", len(p.consumers))

	jsonBytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("error marshaling event: %w", err)
	}

	msg := pluginapi.Message{
		Payload:   jsonBytes,
		Timestamp: time.Now(),
		Metadata: map[string]interface{}{
			"ledger_sequence": event.LedgerSequence,
			"contract_id":     event.ContractID,
			"transaction_id":  event.TransactionID,
			"type":            event.Type,
		},
	}

	for _, consumer := range p.consumers {
		log.Printf("Forwarding to consumer: %s", consumer.Name())
		if err := consumer.Process(ctx, msg); err != nil {
			return fmt.Errorf("error in consumer %s: %w", consumer.Name(), err)
		}
	}
	return nil
}

func (p *ContractEventProcessor) Name() string {
	return "flow/processor/contract-events"
}

func (p *ContractEventProcessor) Version() string {
	return "1.0.0"
}

func (p *ContractEventProcessor) Type() pluginapi.PluginType {
	return pluginapi.ProcessorPlugin
}

func New() pluginapi.Plugin {
	return &ContractEventProcessor{}
}

// filterContractEvents groups contract events by operation index
func filterContractEvents(diagnosticEvents []xdr.DiagnosticEvent) map[int][]xdr.ContractEvent {
	events := make(map[int][]xdr.ContractEvent)

	for _, diagEvent := range diagnosticEvents {
		if !diagEvent.InSuccessfulContractCall || diagEvent.Event.Type != xdr.ContractEventTypeContract {
			continue
		}

		// Use the operation index as the key
		opIndex := 0 // Default to 0 if no specific index available
		events[opIndex] = append(events[opIndex], diagEvent.Event)
	}
	return events
}

func (p *ContractEventProcessor) processContractEvent(
	tx ingest.LedgerTransaction,
	opIndex, eventIndex int,
	event xdr.ContractEvent,
	meta xdr.LedgerCloseMeta,
) (*ContractEvent, error) {
	// Extract contract ID
	var contractID string
	if event.ContractId != nil {
		contractIdByte, err := event.ContractId.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("error marshaling contract ID: %w", err)
		}
		contractID, err = strkey.Encode(strkey.VersionByteContract, contractIdByte)
		if err != nil {
			return nil, fmt.Errorf("error encoding contract ID: %w", err)
		}
	}

	// Get transaction context
	ledgerSequence := meta.LedgerSequence()
	transactionIndex := uint32(tx.Index)
	transactionHash := tx.Result.TransactionHash.HexString()
	transactionID := toid.New(int32(ledgerSequence), int32(transactionIndex), 0).ToInt64()

	// Get close time - converting TimePoint directly to Unix time
	closeTime := time.Unix(int64(meta.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime), 0)

	// Get the event topics
	var topics []xdr.ScVal
	var eventData xdr.ScVal

	if event.Body.V == 0 {
		v0 := event.Body.MustV0()
		topics = v0.Topics
		eventData = v0.Data
	} else {
		return nil, fmt.Errorf("unsupported event body version: %d", event.Body.V)
	}

	// Convert event XDR to base64
	eventXDR, err := xdr.MarshalBase64(event)
	if err != nil {
		return nil, fmt.Errorf("error marshaling event XDR: %w", err)
	}

	// Serialize topics and data
	rawTopics, decodedTopics := serializeScValArray(topics)
	rawData, decodedData := serializeScVal(eventData)

	// Determine if event was in successful transaction
	successful := tx.Result.Successful()

	// Create contract event record with enhanced structure
	contractEvent := &ContractEvent{
		// Transaction context
		TransactionHash:   transactionHash,
		TransactionID:     transactionID,
		Successful:        successful,
		LedgerSequence:    ledgerSequence,
		ClosedAt:          closeTime,
		NetworkPassphrase: p.networkPassphrase,

		// Event context
		ContractID:         contractID,
		EventIndex:         eventIndex,
		OperationIndex:     opIndex,
		InSuccessfulTxCall: successful,

		// Event type information
		Type:     event.Type.String(),
		TypeCode: int32(event.Type),

		// Event data
		Topics:        rawTopics,
		TopicsDecoded: decodedTopics,
		Data:          rawData,
		DataDecoded:   decodedData,

		// Raw XDR
		EventXDR: eventXDR,

		// Metadata for filtering
		Tags: make(map[string]string),
	}

	// Add basic tags for common filtering scenarios
	contractEvent.Tags["contract_id"] = contractID
	contractEvent.Tags["event_type"] = event.Type.String()
	contractEvent.Tags["successful"] = fmt.Sprintf("%t", successful)

	// Add diagnostic events if available
	diagnosticEvents, err := tx.GetDiagnosticEvents()
	if err == nil {
		var diagnosticData []DiagnosticData
		for _, diagEvent := range diagnosticEvents {
			if diagEvent.Event.Type == xdr.ContractEventTypeContract {
				eventData, err := json.Marshal(diagEvent.Event)
				if err != nil {
					continue
				}

				// Get raw XDR for the diagnostic event
				diagXDR, err := xdr.MarshalBase64(diagEvent)
				if err != nil {
					diagXDR = ""
				}

				diagnosticData = append(diagnosticData, DiagnosticData{
					Event:                    eventData,
					InSuccessfulContractCall: diagEvent.InSuccessfulContractCall,
					RawXDR:                   diagXDR,
				})
			}
		}
		contractEvent.DiagnosticEvents = diagnosticData
	}

	return contractEvent, nil
}

// serializeScVal converts an ScVal to structured data format with both raw and decoded representations
func serializeScVal(scVal xdr.ScVal) (EventData, EventData) {
	rawData := EventData{
		Type:  "n/a",
		Value: "n/a",
	}

	decodedData := EventData{
		Type:  "n/a",
		Value: "n/a",
	}

	if scValTypeName, ok := scVal.ArmForSwitch(int32(scVal.Type)); ok {
		rawData.Type = scValTypeName
		decodedData.Type = scValTypeName

		if raw, err := scVal.MarshalBinary(); err == nil {
			rawData.Value = base64.StdEncoding.EncodeToString(raw)
			decodedData.Value = scVal.String()
		}
	}

	return rawData, decodedData
}

// serializeScValArray converts an array of ScVal to structured data format
func serializeScValArray(scVals []xdr.ScVal) ([]TopicData, []TopicData) {
	rawTopics := make([]TopicData, 0, len(scVals))
	decodedTopics := make([]TopicData, 0, len(scVals))

	for _, scVal := range scVals {
		if scValTypeName, ok := scVal.ArmForSwitch(int32(scVal.Type)); ok {
			raw, err := scVal.MarshalBinary()
			if err != nil {
				continue
			}

			rawTopics = append(rawTopics, TopicData{
				Type:  scValTypeName,
				Value: base64.StdEncoding.EncodeToString(raw),
			})

			decodedTopics = append(decodedTopics, TopicData{
				Type:  scValTypeName,
				Value: scVal.String(),
			})
		}
	}

	return rawTopics, decodedTopics
}
