# Setty DTDL Ontology - Buildings
## Based on WillowTwin

This ontology is based on WillowTin's DTDL language ontology. Please visit their (repo)[https://github.com/WillowInc/opendigitaltwins-building] for more information, including an extensive readme.

## Major Changes & Additions

* Boards / Display Boards \
Including SmartBoards, WhiteBoards, TackBoards, and generic InfoBoards. These are placed under in `Asset / Equipment / Board` as they are pieces of equipment used within the building.

* Human Aid assets \
Assets including mobility aids (grab bars, banisters, etc.), restroom aids (toilet seat cover dispensers, soap dispensers, etc.), disposal aids (trash/recycling bins) etc. are placed in `Asset / Human Aid`. These items have a separate category since they are usually miscellaneous.

* Furniture additions \
Including lockers, benches, storage units, podiums, stools, and more. 

* Injury Response equipment \
Created this section under Equipment to denote things like emergency eyewash stations, AEDs, first aid, etc. placed in `Asset / Equipment / Injury Response` A larger "Safety Asset" or "Safety Equipment" section could contain both these assets and the fire protection equipment.

* More Equipment \
Including acoustic equipment (piano, acoustic tiles, etc.), clothing equipment (washer, dryer, racks), engineering equipment (CNC cutter, saws), art equipment (kiln, spraybooth), and more. 