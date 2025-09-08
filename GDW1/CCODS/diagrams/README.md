# Diagram Alternatives Guide

## 🎨 **Why Alternative Formats?**

Mermaid diagrams may not render properly in all Git repository viewers. This folder contains alternative diagram formats that are more universally compatible.

## 📊 **Available Formats**

### **1. PlantUML (.puml files)**
- **Pros**: GitHub native support, professional output, version controllable
- **Cons**: Requires PlantUML syntax knowledge
- **Best for**: Complex flowcharts, sequence diagrams, architecture diagrams
- **Files**: `execution_flow.puml`

**Usage**: 
- GitHub automatically renders `.puml` files
- Can also use online PlantUML editor: http://www.plantuml.com/plantuml/

### **2. ASCII Art (.md files)**
- **Pros**: Universal compatibility, works in any text viewer, lightweight
- **Cons**: Limited visual appeal, manual creation
- **Best for**: Simple diagrams, infrastructure layouts, process flows
- **Files**: `infrastructure_ascii.md`

### **3. Draw.io/Diagrams.net (Recommended)**
- **Pros**: Professional appearance, easy to create, multiple export formats
- **Cons**: Requires external tool, binary files in repo
- **Best for**: Complex architecture diagrams, professional presentations

**Steps to create**:
1. Go to https://app.diagrams.net/
2. Create your diagram
3. Export as SVG (vector, scalable) or PNG (raster, smaller file)
4. Save to this `diagrams/` folder
5. Reference in README: `![Diagram](diagrams/your_diagram.svg)`

### **4. Graphviz DOT Format**
- **Pros**: Code-based, version controllable, automatic layout
- **Cons**: Learning curve, limited GitHub support
- **Best for**: Network diagrams, dependency graphs

### **5. Static Image Files**
- **Pros**: Universal compatibility, professional appearance
- **Cons**: Not version controllable (binary), requires external tools
- **Formats**: PNG, SVG, JPEG
- **Best for**: Final documentation, presentations

## 🔧 **Recommended Workflow**

1. **For Simple Diagrams**: Use ASCII art in markdown files
2. **For Complex Flowcharts**: Use PlantUML (.puml files)
3. **For Architecture Diagrams**: Use Draw.io → export as SVG
4. **For Professional Docs**: Create in Draw.io → export multiple formats

## 📁 **File Organization**

```
diagrams/
├── README.md                    # This guide
├── execution_flow.puml          # PlantUML execution flow
├── infrastructure_ascii.md      # ASCII infrastructure diagram
├── architecture.svg             # (Future) Draw.io architecture
├── monitoring_flow.png          # (Future) Monitoring diagram
└── installation_sequence.dot    # (Future) Graphviz installation
```

## 🔗 **Integration with Main README**

Reference diagrams in the main README like this:

```markdown
### Architecture Overview

> **Diagram Options**: 
> - [Interactive PlantUML](diagrams/architecture.puml)
> - [ASCII Version](diagrams/architecture_ascii.md) 
> - [High-res Image](diagrams/architecture.svg)

![Architecture](diagrams/architecture.svg)
```

## 🛠️ **Tools & Resources**

- **PlantUML**: http://www.plantuml.com/plantuml/
- **Draw.io**: https://app.diagrams.net/
- **ASCII Art Generator**: https://www.asciiart.eu/
- **Graphviz Online**: https://dreampuf.github.io/GraphvizOnline/
- **Mermaid Live Editor**: https://mermaid.live/

## 💡 **Best Practices**

1. **Always provide fallbacks** for Mermaid diagrams
2. **Use SVG format** when possible (scalable, small file size)
3. **Keep ASCII versions** for maximum compatibility
4. **Version control diagram source files** when possible
5. **Use consistent naming** conventions for diagram files
6. **Document diagram updates** in commit messages 