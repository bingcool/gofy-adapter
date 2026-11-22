// Package adapter exposes the version of the gofy-adapter module.
//
// The module is a set of adapters rather than a runnable service, so it has no
// natural owner for a version string; this root package exists for that one
// fact. Applications that log their dependency versions can report
// adapter.Version next to gofy.Version without hard-coding either.
// 包 adapter 暴露 gofy-adapter 模块的版本。
//
// 本模块是一组适配器而不是可运行的服务，版本字符串没有天然归属，这个根包只为这一件事
// 存在。需要记录依赖版本的应用可以在 gofy.Version 旁边报告 adapter.Version，而不必硬编码
// 任何一个。
package adapter

// Version is the gofy-adapter module version. It is the single source of truth
// for the module; change it together with the release notes, not on its own.
// Version 是 gofy-adapter 模块版本，也是本模块的唯一来源；改它要连发布说明一起改，
// 不要单独改动。
const Version = "0.8.0"
