package net.consensys.eventeum.integration.broadcast.blockchain;

import net.consensys.eventeum.dto.block.BlockDetails;
import net.consensys.eventeum.dto.event.ContractEventDetails;
import net.consensys.eventeum.dto.event.filter.ContractEventFilter;
import net.consensys.eventeum.dto.message.BlockEvent;
import net.consensys.eventeum.dto.message.ContractEvent;
import net.consensys.eventeum.dto.message.EventeumMessage;
import net.consensys.eventeum.integration.RabbitSettings;
import net.consensys.eventeum.utils.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.data.repository.CrudRepository;

public class RabbitBlockChainEventBroadcaster implements BlockchainEventBroadcaster {

    private static final Logger LOG = LoggerFactory.getLogger(RabbitBlockChainEventBroadcaster.class);
    private RabbitTemplate rabbitTemplate;
    private RabbitSettings rabbitSettings;

    public RabbitBlockChainEventBroadcaster(RabbitTemplate rabbitTemplate,  RabbitSettings rabbitSettings) {
        this.rabbitTemplate = rabbitTemplate;
        this.rabbitSettings = rabbitSettings;
    }

    @Override
    public void broadcastNewBlock(BlockDetails block) {
        final EventeumMessage<BlockDetails> message = createBlockEventMessage(block);
        LOG.info("Sending message: " + JSON.stringify(message));
    }

    @Override
    public void broadcastContractEvent(ContractEventDetails eventDetails) {
        final EventeumMessage<ContractEventDetails> message = createContractEventMessage(eventDetails);
        rabbitTemplate.convertAndSend(this.rabbitSettings.getExchange(),
                String.format("%s.%s", this.rabbitSettings.getRoutingKeyPrefix(), eventDetails.getFilterId()),
                message);

        LOG.info(String.format("Sending message: [%s] to exchange [%s] with routing key [%s.%s]",
                JSON.stringify(message),
                this.rabbitSettings.getExchange(),
                this.rabbitSettings.getRoutingKeyPrefix(),
                eventDetails.getFilterId()));
    }

    protected EventeumMessage<BlockDetails> createBlockEventMessage(BlockDetails blockDetails) {
        return new BlockEvent(blockDetails);
    }

    protected EventeumMessage<ContractEventDetails> createContractEventMessage(ContractEventDetails contractEventDetails) {
        return new ContractEvent(contractEventDetails);
    }

}
