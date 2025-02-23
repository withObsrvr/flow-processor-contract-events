package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/stellar/go/strkey"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
	"github.com/withObsrvr/pluginapi"
)

// ContractEvent represents an event emitted by a contract
type ContractEvent struct {
	Timestamp         time.Time        `json:"timestamp"`
	LedgerSequence    uint32           `json:"ledger_sequence"`
	TransactionHash   string           `json:"transaction_hash"`
	ContractID        string           `json:"contract_id"`
	Type              string           `json:"type"`
	Topic             []xdr.ScVal      `json:"topic"`
	Data              json.RawMessage  `json:"data"`
	InSuccessfulTx    bool             `json:"in_successful_tx"`
	EventIndex        int              `json:"event_index"`
	OperationIndex    int              `json:"operation_index"`
	DiagnosticEvents  []DiagnosticData `json:"diagnostic_events,omitempty"`
	NetworkPassphrase string           `json:"network_passphrase"`
}

type DiagnosticData struct {
	Event                    json.RawMessage `json:"event"`
	InSuccessfulContractCall bool            `json:"in_successful_contract_call"`
}

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

	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading transaction: %w", err)
		}

		diagnosticEvents, err := tx.GetDiagnosticEvents()
		if err != nil {
			log.Printf("Error getting diagnostic events: %v", err)
			continue
		}

		for opIdx, events := range filterContractEvents(diagnosticEvents) {
			for eventIdx, event := range events {
				contractEvent, err := p.processContractEvent(tx, opIdx, eventIdx, event, ledgerCloseMeta)
				if err != nil {
					log.Printf("Error processing contract event: %v", err)
					continue
				}

				if contractEvent != nil {
					if err := p.forwardToConsumers(ctx, contractEvent); err != nil {
						log.Printf("Error forwarding event: %v", err)
					}
				}
			}
		}
	}

	p.mu.Lock()
	p.stats.ProcessedLedgers++
	p.stats.LastLedger = sequence
	p.stats.LastProcessedTime = time.Now()
	p.mu.Unlock()

	return nil
}

func (p *ContractEventProcessor) forwardToConsumers(ctx context.Context, event *ContractEvent) error {
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
		},
	}

	for _, consumer := range p.consumers {
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
	contractID, err := strkey.Encode(strkey.VersionByteContract, event.ContractId[:])
	if err != nil {
		return nil, fmt.Errorf("error encoding contract ID: %w", err)
	}

	// Convert event body to JSON
	data, err := json.Marshal(event.Body)
	if err != nil {
		return nil, fmt.Errorf("error marshaling event data: %w", err)
	}

	// Determine if event was in successful transaction
	successful := tx.Result.Successful()

	p.mu.Lock()
	p.stats.EventsFound++
	if successful {
		p.stats.SuccessfulEvents++
	} else {
		p.stats.FailedEvents++
	}
	p.mu.Unlock()

	// Create contract event record
	contractEvent := &ContractEvent{
		Timestamp:         time.Unix(int64(meta.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime), 0),
		LedgerSequence:    meta.LedgerSequence(),
		TransactionHash:   tx.Result.TransactionHash.HexString(),
		ContractID:        contractID,
		Type:              string(event.Type),
		Topic:             event.Body.V0.Topics,
		Data:              data,
		InSuccessfulTx:    successful,
		EventIndex:        eventIndex,
		OperationIndex:    opIndex,
		NetworkPassphrase: p.networkPassphrase,
	}

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
				diagnosticData = append(diagnosticData, DiagnosticData{
					Event:                    eventData,
					InSuccessfulContractCall: diagEvent.InSuccessfulContractCall,
				})
			}
		}
		contractEvent.DiagnosticEvents = diagnosticData
	}

	return contractEvent, nil
}
