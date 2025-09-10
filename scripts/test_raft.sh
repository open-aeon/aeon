#!/bin/bash

# Bifrost Raft 引擎测试脚本
# 
# 这个脚本提供了多种测试选项来验证我们的 Raft 引擎

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 打印带颜色的消息
print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_header() {
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}$(echo "$1" | sed 's/./=/g')${NC}"
}

# 显示帮助信息
show_help() {
    echo "Bifrost Raft 引擎测试脚本"
    echo ""
    echo "用法: $0 [选项]"
    echo ""
    echo "选项:"
    echo "  -h, --help     显示此帮助信息"
    echo "  -a, --all      运行所有测试"
    echo "  -u, --unit     运行单元测试"
    echo "  -i, --integration 运行集成测试"
    echo "  -d, --demo     运行演示程序"
    echo "  -b, --bench    运行性能基准测试"
    echo "  -c, --clean    清理构建缓存"
    echo "  -v, --verbose  详细输出"
    echo ""
    echo "示例:"
    echo "  $0 --all       # 运行所有测试"
    echo "  $0 --demo      # 运行演示程序"
    echo "  $0 --unit -v   # 运行单元测试（详细输出）"
}

# 检查 Rust 环境
check_rust_env() {
    if ! command -v cargo &> /dev/null; then
        print_error "Cargo 未找到，请安装 Rust"
        exit 1
    fi
    
    print_info "Rust 版本: $(rustc --version)"
    print_info "Cargo 版本: $(cargo --version)"
}

# 运行单元测试
run_unit_tests() {
    print_header "运行单元测试"
    
    print_info "运行 Raft 存储层测试..."
    cargo test --lib raft::storage $VERBOSE_FLAG
    
    print_info "运行 Raft 适配器测试..."
    cargo test --lib raft::adapter $VERBOSE_FLAG
    
    print_info "运行 Raft 管理器测试..."
    cargo test --lib raft::manager $VERBOSE_FLAG
    
    print_success "单元测试完成"
}

# 运行集成测试
run_integration_tests() {
    print_header "运行集成测试"
    
    print_info "运行 Raft 引擎集成测试..."
    cargo test --test raft_engine_test $VERBOSE_FLAG
    
    print_success "集成测试完成"
}

# 运行演示程序
run_demo() {
    print_header "运行 Raft 引擎演示"
    
    print_info "启动演示程序..."
    cargo run --example raft_demo
    
    print_success "演示程序完成"
}

# 运行性能基准测试
run_benchmark() {
    print_header "运行性能基准测试"
    
    print_info "运行性能基准测试..."
    cargo test --test raft_engine_test test_performance_benchmark --release $VERBOSE_FLAG
    
    print_success "性能基准测试完成"
}

# 清理构建缓存
clean_build() {
    print_header "清理构建缓存"
    
    print_info "清理 Cargo 缓存..."
    cargo clean
    
    print_success "构建缓存已清理"
}

# 运行所有测试
run_all_tests() {
    print_header "运行所有测试"
    
    run_unit_tests
    echo ""
    run_integration_tests
    echo ""
    run_benchmark
    echo ""
    run_demo
    
    print_success "所有测试完成"
}

# 主函数
main() {
    local run_all=false
    local run_unit=false
    local run_integration=false
    local run_demo_flag=false
    local run_bench=false
    local clean_flag=false
    local verbose=false
    
    # 解析命令行参数
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                show_help
                exit 0
                ;;
            -a|--all)
                run_all=true
                shift
                ;;
            -u|--unit)
                run_unit=true
                shift
                ;;
            -i|--integration)
                run_integration=true
                shift
                ;;
            -d|--demo)
                run_demo_flag=true
                shift
                ;;
            -b|--bench)
                run_bench=true
                shift
                ;;
            -c|--clean)
                clean_flag=true
                shift
                ;;
            -v|--verbose)
                verbose=true
                shift
                ;;
            *)
                print_error "未知选项: $1"
                show_help
                exit 1
                ;;
        esac
    done
    
    # 设置详细输出标志
    if [ "$verbose" = true ]; then
        VERBOSE_FLAG="-- --nocapture"
    else
        VERBOSE_FLAG=""
    fi
    
    # 检查 Rust 环境
    check_rust_env
    echo ""
    
    # 如果没有指定任何选项，显示帮助
    if [ "$run_all" = false ] && [ "$run_unit" = false ] && [ "$run_integration" = false ] && \
       [ "$run_demo_flag" = false ] && [ "$run_bench" = false ] && [ "$clean_flag" = false ]; then
        show_help
        exit 0
    fi
    
    # 执行清理
    if [ "$clean_flag" = true ]; then
        clean_build
        echo ""
    fi
    
    # 执行测试
    if [ "$run_all" = true ]; then
        run_all_tests
    else
        if [ "$run_unit" = true ]; then
            run_unit_tests
            echo ""
        fi
        
        if [ "$run_integration" = true ]; then
            run_integration_tests
            echo ""
        fi
        
        if [ "$run_bench" = true ]; then
            run_benchmark
            echo ""
        fi
        
        if [ "$run_demo_flag" = true ]; then
            run_demo
            echo ""
        fi
    fi
    
    print_success "测试脚本执行完成！"
}

# 运行主函数
main "$@"
