from bot_manager.strategies.aggressive_twap import AggressiveTWAPStrategy
from bot_manager.strategies.aggressive_twap_v2 import AggressiveTWAPV2Strategy
from bot_manager.strategies.passive_quoting import PassiveQuotingStrategy
from bot_manager.strategies.top_of_book import TopOfBookStrategy
from bot_manager.strategies.targeted_buybot import TargetedBuybotStrategy
from bot_manager.strategies.targeted_sellbot import TargetedSellbotStrategy
from bot_manager.strategies.volume_weighted_top_of_book import VolumeWeightedTopOfBookStrategy
from bot_manager.strategies.volume_weighted_top_of_book_delta import VolumeWeightedTopOfBookStrategy as VolumeWeightedTopOfBookDeltaStrategy
from bot_manager.strategies.stacked_market_making import StackedMarketMakingStrategy
from bot_manager.strategies.stacked_market_making_delta import StackedMarketMakingStrategy as StackedMarketMakingDeltaStrategy

STRATEGY_MAPPING = {
    'aggressive_twap': AggressiveTWAPStrategy,
    'aggressive_twap_v2': AggressiveTWAPV2Strategy,
    'passive_quoting': PassiveQuotingStrategy,
    'top_of_book': TopOfBookStrategy,
    'targeted_buybot': TargetedBuybotStrategy,
    'targeted_sellbot': TargetedSellbotStrategy,
    'volume_weighted_top_of_book': VolumeWeightedTopOfBookStrategy,
    'volume_weighted_top_of_book_delta': VolumeWeightedTopOfBookDeltaStrategy,
    'stacked_market_making': StackedMarketMakingStrategy,
    'stacked_market_making_delta': StackedMarketMakingDeltaStrategy
}
