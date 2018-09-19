package net.consensys.eventeum.integration.broadcast.blockchain;

import net.consensys.eventeum.dto.block.BlockDetails;
import net.consensys.eventeum.dto.event.ContractEventDetails;
import net.consensys.eventeum.dto.event.filter.ContractEventFilter;
import net.consensys.eventeum.dto.message.BlockEvent;
import net.consensys.eventeum.dto.message.ContractEvent;
import net.consensys.eventeum.dto.message.EventeumMessage;
import net.consensys.eventeum.utils.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.repository.CrudRepository;

public class LoggerBlockChainEventBroadcaster implements BlockchainEventBroadcaster {

    private static final Logger LOG = LoggerFactory.getLogger(LoggerBlockChainEventBroadcaster.class);

    private CrudRepository<ContractEventFilter, String> filterRespository;

    public LoggerBlockChainEventBroadcaster(CrudRepository<ContractEventFilter, String> filterRespository) {
        this.filterRespository = filterRespository;
    }

    @Override
    public void broadcastNewBlock(BlockDetails block) {
        final EventeumMessage<BlockDetails> message = createBlockEventMessage(block);
        LOG.info("Sending message: " + JSON.stringify(message));
    }

    @Override
    public void broadcastContractEvent(ContractEventDetails eventDetails) {
        final EventeumMessage<ContractEventDetails> message = createContractEventMessage(eventDetails);
        LOG.info("Sending message: " + JSON.stringify(message));
    }

    protected EventeumMessage<BlockDetails> createBlockEventMessage(BlockDetails blockDetails) {
        return new BlockEvent(blockDetails);
    }

    protected EventeumMessage<ContractEventDetails> createContractEventMessage(ContractEventDetails contractEventDetails) {
        return new ContractEvent(contractEventDetails);
    }

    private String getContractEventCorrelationId(EventeumMessage<ContractEventDetails> message) {
        final ContractEventFilter filter = filterRespository.findOne(message.getDetails().getFilterId());

        if (filter == null || filter.getCorrelationIdStrategy() == null) {
            return message.getId();
        }

        return filter.getCorrelationIdStrategy().getCorrelationId(message.getDetails());
    }
}
