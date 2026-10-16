// swift-tools-version:5.9
import PackageDescription
let package = Package(name: "Trial", platforms: [.iOS(.v16), .macOS(.v13)],
  products: [.library(name: "Trial", targets: ["Trial"])],
  targets: [.target(name: "Trial"), .testTarget(name: "TrialTests", dependencies: ["Trial"])])
