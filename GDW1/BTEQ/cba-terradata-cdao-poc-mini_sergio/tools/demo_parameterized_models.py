#!/usr/bin/env python3
"""
Demonstration of Parameterized Model Configuration

Shows how to use the new parameter-driven model system without hard-coding model names.
"""

import sys
from pathlib import Path

# Add bteq_dcf to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def demo_model_configuration():
    """Demonstrate the parameterized model configuration system."""
    print("🎯 Parameterized Model Configuration Demo")
    print("=" * 45)
    
    try:
        from bteq_dcf.utils.config import (
            get_model_manager, TaskType, ModelSelectionStrategy,
            get_default_model, get_models_for_task
        )
        
        # Initialize model manager
        model_manager = get_model_manager()
        
        print("1. 📋 Model Configuration Overview")
        print("-" * 35)
        
        # Show loaded configuration
        stats = model_manager.get_model_statistics()
        print(f"   Total Models: {stats['total_models']}")
        print(f"   Providers: {', '.join(stats['providers'])}")
        print(f"   Default Pair: {stats['default_pair']}")
        print(f"   Multi-Model Enabled: {stats['multi_model_enabled']}")
        print(f"   Average Quality Score: {stats['average_quality_score']:.2f}")
        
        print("\n2. 🎯 Task-Specific Model Selection")
        print("-" * 35)
        
        # Demonstrate task-specific model selection
        tasks = [
            TaskType.BTEQ_ANALYSIS,
            TaskType.CODE_GENERATION,
            TaskType.ERROR_CORRECTION,
            TaskType.PERFORMANCE_OPTIMIZATION
        ]
        
        for task in tasks:
            models = get_models_for_task(task)
            print(f"   {task.value:25} → {models}")
        
        print("\n3. 🔄 Model Selection Strategies")
        print("-" * 35)
        
        # Demonstrate different selection strategies
        strategies = [
            ModelSelectionStrategy.QUALITY_FIRST,
            ModelSelectionStrategy.PERFORMANCE_FIRST,
            ModelSelectionStrategy.BALANCED,
            ModelSelectionStrategy.COST_OPTIMIZED
        ]
        
        task = TaskType.CODE_GENERATION
        for strategy in strategies:
            ranked_models = model_manager.rank_models_for_task(task, strategy)
            print(f"   {strategy.value:15} → {ranked_models}")
        
        print("\n4. 📊 Model Information Details")
        print("-" * 30)
        
        # Show detailed model information
        default_pair = model_manager.get_default_pair()
        for model_name in default_pair:
            model_info = model_manager.get_model_info(model_name)
            if model_info:
                print(f"\n   📋 {model_name}:")
                print(f"      Provider: {model_info.provider}")
                print(f"      Quality Score: {model_info.quality_score}")
                print(f"      Speed: {model_info.speed}")
                print(f"      Cost: {model_info.cost}")
                print(f"      Capabilities: {', '.join(model_info.capabilities)}")
        
        print("\n5. 🧠 Complexity-Based Selection")
        print("-" * 30)
        
        # Demonstrate complexity-based model selection
        complexity_scores = [0.3, 0.7, 0.9]
        for score in complexity_scores:
            selected_model = model_manager.select_model_for_complexity(score)
            print(f"   Complexity {score:.1f} → {selected_model}")
        
        print("\n6. ✅ Model Validation")
        print("-" * 20)
        
        # Validate model availability
        test_models = ["claude-4-sonnet", "snowflake-llama-3.3-70b", "invalid-model"]
        available, unavailable = model_manager.validate_model_availability(test_models)
        
        print(f"   Available: {available}")
        print(f"   Unavailable: {unavailable}")
        
        return True
        
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def demo_usage_patterns():
    """Demonstrate common usage patterns."""
    print("\n🔧 Common Usage Patterns")
    print("=" * 30)
    
    try:
        from bteq_dcf.utils.config import get_model_manager, TaskType
        from snowflake.cortex import complete
        from bteq_dcf.utils.database import get_connection_manager
        
        model_manager = get_model_manager()
        connection_manager = get_connection_manager()
        session = connection_manager.get_session()
        
        print("1. 🎯 Task-Driven Model Selection")
        print("-" * 35)
        
        # Example: Select best model for BTEQ analysis
        analysis_models = model_manager.get_models_for_task(TaskType.BTEQ_ANALYSIS)
        best_analysis_model = analysis_models[0] if analysis_models else model_manager.get_default_model()
        
        print(f"   Task: BTEQ Analysis")
        print(f"   Selected Model: {best_analysis_model}")
        
        # Test the selected model
        try:
            test_prompt = "Rate this BTEQ complexity (1-10): DELETE FROM table; INSERT INTO table SELECT * FROM source;"
            response = complete(best_analysis_model, test_prompt, session=session)
            print(f"   ✅ Response: {response[:60]}...")
        except Exception as e:
            print(f"   ❌ Test failed: {e}")
        
        print("\n2. 🔄 Multi-Model Comparison")
        print("-" * 30)
        
        # Example: Compare multiple models for code generation
        generation_models = model_manager.get_models_for_task(TaskType.CODE_GENERATION)
        
        test_prompt = "Convert: DELETE FROM target; to Snowflake stored procedure"
        
        for model in generation_models[:2]:  # Test first 2 models
            try:
                print(f"\n   🤖 Testing {model}...")
                response = complete(model, test_prompt, session=session)
                print(f"   ✅ Response length: {len(response)} chars")
                print(f"   📝 Preview: {response[:50]}...")
            except Exception as e:
                print(f"   ❌ {model} failed: {e}")
        
        print("\n3. 📈 Adaptive Model Selection")
        print("-" * 30)
        
        # Example: Select model based on complexity
        complexity_scenarios = [
            ("Simple DELETE/INSERT", 0.3),
            ("Complex JOIN with window functions", 0.8),
            ("Recursive CTE with error handling", 0.9)
        ]
        
        for scenario, complexity in complexity_scenarios:
            selected_model = model_manager.select_model_for_complexity(complexity)
            print(f"   {scenario} ({complexity}) → {selected_model}")
        
        print("\n✨ Configuration-Driven Benefits:")
        print("   • No hard-coded model names in code")
        print("   • Easy to add/remove models")
        print("   • Task-specific optimization")
        print("   • Performance and cost control")
        print("   • Centralized model management")
        
        return True
        
    except Exception as e:
        print(f"❌ Usage demo failed: {e}")
        return False


if __name__ == "__main__":
    print("🚀 Starting Parameterized Model Demo")
    print()
    
    # Run demonstrations
    config_success = demo_model_configuration()
    usage_success = demo_usage_patterns()
    
    overall_success = config_success and usage_success
    
    print(f"\n{'='*50}")
    print("🏁 DEMO RESULTS")
    print(f"{'='*50}")
    print(f"Configuration Demo: {'✅ SUCCESS' if config_success else '❌ FAILED'}")
    print(f"Usage Patterns Demo: {'✅ SUCCESS' if usage_success else '❌ FAILED'}")
    print(f"Overall: {'🎉 COMPLETE' if overall_success else '🚨 ISSUES'}")
    
    if overall_success:
        print("\n💡 Next Steps:")
        print("   • Modify config/models.json to add/change models")
        print("   • Use TaskType enum for model selection")
        print("   • Leverage automatic model routing")
        print("   • Monitor performance via model statistics")
    else:
        print("\n🔧 Check configuration files and model availability")
    
    sys.exit(0 if overall_success else 1)
